package resolver

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"

	"github.com/spf13/viper"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type noopLogger struct{}

func (noopLogger) Debug(string, ...any)                   {}
func (noopLogger) Info(string, ...any)                    {}
func (noopLogger) Warn(string, ...any)                    {}
func (noopLogger) Error(string, ...any)                   {}
func (noopLogger) Fatal(string, ...any)                   {}
func (noopLogger) WithOutput(io.Writer) interfaces.Logger { return noopLogger{} }

type fakeClientConn struct {
	mu         sync.Mutex
	states     []resolver.State
	reportErrs []error
}

func (f *fakeClientConn) UpdateState(s resolver.State) error {
	f.mu.Lock()
	f.states = append(f.states, s)
	f.mu.Unlock()
	return nil
}

func (f *fakeClientConn) ReportError(err error) {
	f.mu.Lock()
	f.reportErrs = append(f.reportErrs, err)
	f.mu.Unlock()
}

func (f *fakeClientConn) NewAddress([]resolver.Address) {}

func (f *fakeClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

func TestGRPCResolverScheme_IsURLSafe(t *testing.T) {
	for _, comp := range []api.Component{
		api.Component_COMPONENT_QUEUE,
		api.Component_COMPONENT_LOG,
	} {
		s := grpcResolverScheme(comp)
		for _, r := range s {
			if r == '_' {
				t.Fatalf("scheme %q must not contain underscore (breaks grpc URL parsing / DNS fallback)", s)
			}
		}
	}
	if grpcResolverScheme(api.Component_COMPONENT_QUEUE) != "vectis-queue" {
		t.Fatalf("queue scheme: %s", grpcResolverScheme(api.Component_COMPONENT_QUEUE))
	}
}

func TestGRPCHealthServiceName_MatchesServers(t *testing.T) {
	if grpcHealthServiceName(api.Component_COMPONENT_QUEUE) != "queue" {
		t.Fatal("queue health name must match internal/queue RegisterQueueService")
	}
	if grpcHealthServiceName(api.Component_COMPONENT_LOG) != "log" {
		t.Fatal("log health name must match internal/logserver RunGRPC")
	}
}

func TestStaticBuilder_Build(t *testing.T) {
	cc := &fakeClientConn{}
	b := &staticBuilder{addr: "127.0.0.1:9999", logger: noopLogger{}}
	_, err := b.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if len(cc.states) != 1 || len(cc.states[0].Addresses) != 1 || cc.states[0].Addresses[0].Addr != "127.0.0.1:9999" {
		t.Fatalf("unexpected state: %+v", cc.states)
	}
}

type mockRegistry struct {
	addr string
	err  error
}

func (m *mockRegistry) Address(context.Context, api.Component) (string, error) {
	return m.addr, m.err
}

func TestRegistryResolver_ResolveAndFallback(t *testing.T) {
	cc := &fakeClientConn{}
	reg := &mockRegistry{addr: "first:1", err: nil}
	r := newRegistryResolver(cc, reg, api.Component_COMPONENT_QUEUE, noopLogger{}, time.Hour, 5*time.Second, time.Second)

	r.ResolveNow(resolver.ResolveNowOptions{})
	if len(cc.states) == 0 {
		t.Fatal("expected UpdateState after resolve")
	}

	reg.addr = ""
	reg.err = errors.New("boom")
	r.ResolveNow(resolver.ResolveNowOptions{})

	if got := cc.states[len(cc.states)-1].Addresses[0].Addr; got != "first:1" {
		t.Fatalf("expected fallback to first:1, last state addresses: %+v", cc.states[len(cc.states)-1].Addresses)
	}

	r.Close()
}

func TestRegistryResolver_LastGoodPersistsAfterInitialSuccess(t *testing.T) {
	cc := &fakeClientConn{}
	reg := &mockRegistry{addr: "stable:1", err: nil}
	r := newRegistryResolver(cc, reg, api.Component_COMPONENT_QUEUE, noopLogger{}, time.Hour, 5*time.Second, time.Second)

	r.ResolveNow(resolver.ResolveNowOptions{})
	if len(cc.states) == 0 || cc.states[0].Addresses[0].Addr != "stable:1" {
		t.Fatalf("expected initial resolve to stable:1, got %+v", cc.states)
	}

	reg.addr = ""
	reg.err = errors.New("registry unavailable")
	for range 3 {
		r.ResolveNow(resolver.ResolveNowOptions{})
	}
	last := cc.states[len(cc.states)-1]
	if len(last.Addresses) != 1 || last.Addresses[0].Addr != "stable:1" {
		t.Fatalf("expected lastGood stable:1 after repeated errors, got %+v", last.Addresses)
	}

	r.Close()
}

func TestRegistryResolver_ReportErrorOnFailure(t *testing.T) {
	cc := &fakeClientConn{}
	reg := &mockRegistry{addr: "", err: errors.New("registry down")}
	r := newRegistryResolver(cc, reg, api.Component_COMPONENT_QUEUE, noopLogger{}, time.Hour, 5*time.Second, time.Second)

	r.ResolveNow(resolver.ResolveNowOptions{})
	if len(cc.reportErrs) == 0 {
		t.Fatal("expected ReportError when discovery fails")
	}

	r.Close()
}

func TestRegistryResolver_NoReportErrorWhenFallingBackToLastGood(t *testing.T) {
	cc := &fakeClientConn{}
	reg := &mockRegistry{addr: "live:1", err: nil}
	r := newRegistryResolver(cc, reg, api.Component_COMPONENT_QUEUE, noopLogger{}, time.Hour, 5*time.Second, time.Second)

	r.ResolveNow(resolver.ResolveNowOptions{})
	reg.err = errors.New("registry down")
	r.ResolveNow(resolver.ResolveNowOptions{})

	if len(cc.reportErrs) != 0 {
		t.Fatalf("expected no ReportError when lastGood fallback keeps addresses valid, got %v", cc.reportErrs)
	}

	r.Close()
}

func TestPinnedAddress_QueuePrecedence(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("queue.resolver.address", ":resolver")
	viper.Set("worker.queue.address", ":worker")
	viper.Set("api.queue.address", ":api")
	if got := pinnedAddress(api.Component_COMPONENT_QUEUE); got != ":resolver" {
		t.Fatalf("queue.resolver: got %q", got)
	}

	viper.Set("queue.resolver.address", "")
	if got := pinnedAddress(api.Component_COMPONENT_QUEUE); got != ":worker" {
		t.Fatalf("worker.queue: got %q", got)
	}

	viper.Set("worker.queue.address", "")
	if got := pinnedAddress(api.Component_COMPONENT_QUEUE); got != ":api" {
		t.Fatalf("api.queue: got %q", got)
	}
}

func TestLogResolverAddress_Key(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("log.grpc.resolver.address", ":grpc")
	if got := config.LogResolverAddress(); got != ":grpc" {
		t.Fatalf("log.grpc.resolver.address: got %q", got)
	}

	viper.Set("log.grpc.resolver.address", "")
	viper.Set("log.resolver.address", ":legacy")
	if got := config.LogResolverAddress(); got != ":legacy" {
		t.Fatalf("log.resolver.address legacy: got %q", got)
	}
}

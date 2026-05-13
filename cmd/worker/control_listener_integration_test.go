//go:build integration

package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestStartControlListener_EphemeralReturnsDialableAddress(t *testing.T) {
	skipIfControlListenUnavailable(t)

	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "ephemeral")

	ln, addr, err := startControlListener()
	if err != nil {
		t.Fatalf("startControlListener(ephemeral): %v", err)
	}
	defer ln.Close()

	assertDialableControlAddress(t, addr)
}

func TestStartControlListener_RangeReturnsDialableAddress(t *testing.T) {
	skipIfControlListenUnavailable(t)

	var blocker net.Listener
	var ln net.Listener
	var addr string
	var expectedPort int

	for i := 0; i < 128; i++ {
		viper.Reset()

		first, firstPort, secondPort, ok := reserveAdjacentControlPorts(t)
		if !ok {
			continue
		}

		viper.Set("worker.control.mode", "range")
		viper.Set("control_port_min", firstPort)
		viper.Set("control_port_max", secondPort)

		var err error
		ln, addr, err = startControlListener()
		if err == nil {
			blocker = first
			expectedPort = secondPort
			break
		}

		_ = first.Close()
		if !strings.Contains(err.Error(), "no available port in range") {
			t.Fatalf("startControlListener(range): %v", err)
		}
	}
	t.Cleanup(viper.Reset)

	if ln == nil {
		t.Skip("no stable adjacent TCP ports available for control listener integration test")
	}
	defer blocker.Close()
	defer ln.Close()

	if gotPort := assertDialableControlAddress(t, addr); gotPort != expectedPort {
		t.Fatalf("control listener port = %d, want %d", gotPort, expectedPort)
	}
}

func skipIfControlListenUnavailable(t *testing.T) {
	t.Helper()

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Skipf("TCP listen unavailable in this environment: %v", err)
	}
	_ = ln.Close()
}

func reserveAdjacentControlPorts(t *testing.T) (net.Listener, int, int, bool) {
	t.Helper()

	first, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Skipf("TCP listen unavailable in this environment: %v", err)
	}

	firstPort := first.Addr().(*net.TCPAddr).Port
	if firstPort >= 65535 {
		_ = first.Close()
		return nil, 0, 0, false
	}

	secondPort := firstPort + 1
	second, err := net.Listen("tcp", fmt.Sprintf(":%d", secondPort))
	if err != nil {
		_ = first.Close()
		return nil, 0, 0, false
	}
	_ = second.Close()

	return first, firstPort, secondPort, true
}

func assertDialableControlAddress(t *testing.T, addr string) int {
	t.Helper()

	if strings.HasPrefix(addr, ":") {
		t.Fatalf("expected dialable host:port address, got %q", addr)
	}

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected returned address to be dialable, got error: %v", err)
	}
	_ = conn.Close()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("control address %q is not host:port: %v", addr, err)
	}
	gotPort, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("control address %q has non-numeric port %q", addr, port)
	}

	return gotPort
}

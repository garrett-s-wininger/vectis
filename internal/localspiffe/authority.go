package localspiffe

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	workloadpb "github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	spiretypes "github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultX509SVIDTTL = 5 * time.Minute

type Config struct {
	TrustDomain            string
	DataDir                string
	RuntimeDir             string
	WorkloadSocketPath     string
	RegistrationSocketPath string
	BundleFile             string
	Selectors              []string
	DefaultX509SVIDTTL     time.Duration
}

type Authority struct {
	workloadpb.UnimplementedSpiffeWorkloadAPIServer
	entryv1.UnimplementedEntryServer

	cfg                  Config
	caCert               *x509.Certificate
	caKey                *ecdsa.PrivateKey
	callerSelectorSet    map[string]struct{}
	workloadServer       *grpc.Server
	registrationServer   *grpc.Server
	workloadListener     net.Listener
	registrationListener net.Listener
	stopOnce             sync.Once

	mu            sync.Mutex
	entriesByID   map[string]*spiretypes.Entry
	entryIDsByKey map[string]string
	keysByEntryID map[string]string
}

func Start(ctx context.Context, cfg Config) (*Authority, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.DefaultX509SVIDTTL == 0 {
		cfg.DefaultX509SVIDTTL = defaultX509SVIDTTL
	}

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("local spiffe: create data dir: %w", err)
	}
	if err := os.MkdirAll(cfg.RuntimeDir, 0o700); err != nil {
		return nil, fmt.Errorf("local spiffe: create runtime dir: %w", err)
	}

	caCert, caKey, err := ensureCA(cfg)
	if err != nil {
		return nil, err
	}
	if err := writeBundleFile(cfg.BundleFile, caCert.Raw); err != nil {
		return nil, err
	}

	a := &Authority{
		cfg:               cfg,
		caCert:            caCert,
		caKey:             caKey,
		callerSelectorSet: makeSelectorSet(cfg.Selectors),
		entriesByID:       map[string]*spiretypes.Entry{},
		entryIDsByKey:     map[string]string{},
		keysByEntryID:     map[string]string{},
	}

	if err := a.startServers(ctx); err != nil {
		a.Stop()
		return nil, err
	}

	return a, nil
}

func (a *Authority) Stop() {
	if a == nil {
		return
	}

	a.stopOnce.Do(func() {
		if a.workloadServer != nil {
			a.workloadServer.Stop()
		}
		if a.registrationServer != nil {
			a.registrationServer.Stop()
		}
		if a.workloadListener != nil {
			_ = a.workloadListener.Close()
		}
		if a.registrationListener != nil {
			_ = a.registrationListener.Close()
		}
		_ = os.Remove(a.cfg.WorkloadSocketPath)
		_ = os.Remove(a.cfg.RegistrationSocketPath)
	})
}

func (a *Authority) FetchX509SVID(_ *workloadpb.X509SVIDRequest, stream grpc.ServerStreamingServer[workloadpb.X509SVIDResponse]) error {
	if a == nil {
		return status.Error(codes.Unavailable, "local spiffe authority is not configured")
	}

	entries := a.matchingEntries(time.Now().UTC())
	svids := make([]*workloadpb.X509SVID, 0, len(entries))
	for _, entry := range entries {
		svid, err := a.signX509SVID(entry, time.Now().UTC())
		if err != nil {
			return status.Errorf(codes.Internal, "mint X.509-SVID: %v", err)
		}
		svids = append(svids, svid)
	}

	return stream.Send(&workloadpb.X509SVIDResponse{Svids: svids})
}

func (a *Authority) FetchX509Bundles(_ *workloadpb.X509BundlesRequest, stream grpc.ServerStreamingServer[workloadpb.X509BundlesResponse]) error {
	if a == nil {
		return status.Error(codes.Unavailable, "local spiffe authority is not configured")
	}

	return stream.Send(&workloadpb.X509BundlesResponse{
		Bundles: map[string][]byte{a.cfg.TrustDomain: a.caCert.Raw},
	})
}

func (a *Authority) BatchCreateEntry(_ context.Context, req *entryv1.BatchCreateEntryRequest) (*entryv1.BatchCreateEntryResponse, error) {
	if a == nil {
		return nil, status.Error(codes.Unavailable, "local spiffe authority is not configured")
	}

	now := time.Now().UTC()
	resp := &entryv1.BatchCreateEntryResponse{Results: make([]*entryv1.BatchCreateEntryResponse_Result, 0, len(req.GetEntries()))}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredLocked(now)

	for _, incoming := range req.GetEntries() {
		entry, err := a.prepareEntry(incoming, now)
		if err != nil {
			resp.Results = append(resp.Results, &entryv1.BatchCreateEntryResponse_Result{Status: apiStatus(codes.InvalidArgument, err.Error())})
			continue
		}

		key := entryKey(entry)
		if existingID, ok := a.entryIDsByKey[key]; ok {
			resp.Results = append(resp.Results, &entryv1.BatchCreateEntryResponse_Result{
				Status: apiStatus(codes.AlreadyExists, "entry already exists"),
				Entry:  cloneEntry(a.entriesByID[existingID]),
			})
			continue
		}

		a.entriesByID[entry.GetId()] = entry
		a.entryIDsByKey[key] = entry.GetId()
		a.keysByEntryID[entry.GetId()] = key
		resp.Results = append(resp.Results, &entryv1.BatchCreateEntryResponse_Result{
			Status: apiStatus(codes.OK, ""),
			Entry:  cloneEntry(entry),
		})
	}

	return resp, nil
}

func (a *Authority) BatchUpdateEntry(_ context.Context, req *entryv1.BatchUpdateEntryRequest) (*entryv1.BatchUpdateEntryResponse, error) {
	if a == nil {
		return nil, status.Error(codes.Unavailable, "local spiffe authority is not configured")
	}

	now := time.Now().UTC()
	resp := &entryv1.BatchUpdateEntryResponse{Results: make([]*entryv1.BatchUpdateEntryResponse_Result, 0, len(req.GetEntries()))}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredLocked(now)

	for _, incoming := range req.GetEntries() {
		id := strings.TrimSpace(incoming.GetId())
		if id == "" {
			resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{Status: apiStatus(codes.InvalidArgument, "entry ID is required")})
			continue
		}

		existing, ok := a.entriesByID[id]
		if !ok {
			resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{Status: apiStatus(codes.NotFound, "entry not found")})
			continue
		}

		updated := cloneEntry(existing)
		applyEntryUpdate(updated, incoming, req.GetInputMask())
		if err := a.validateEntry(updated, now); err != nil {
			resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{Status: apiStatus(codes.InvalidArgument, err.Error())})
			continue
		}
		updated.RevisionNumber = existing.GetRevisionNumber() + 1

		oldKey := a.keysByEntryID[id]
		newKey := entryKey(updated)
		if oldKey != newKey {
			if existingID, exists := a.entryIDsByKey[newKey]; exists && existingID != id {
				resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{Status: apiStatus(codes.AlreadyExists, "entry already exists")})
				continue
			}
			delete(a.entryIDsByKey, oldKey)
			a.entryIDsByKey[newKey] = id
			a.keysByEntryID[id] = newKey
		}

		a.entriesByID[id] = updated
		resp.Results = append(resp.Results, &entryv1.BatchUpdateEntryResponse_Result{
			Status: apiStatus(codes.OK, ""),
			Entry:  cloneEntry(updated),
		})
	}

	return resp, nil
}

func (a *Authority) BatchDeleteEntry(_ context.Context, req *entryv1.BatchDeleteEntryRequest) (*entryv1.BatchDeleteEntryResponse, error) {
	if a == nil {
		return nil, status.Error(codes.Unavailable, "local spiffe authority is not configured")
	}

	resp := &entryv1.BatchDeleteEntryResponse{Results: make([]*entryv1.BatchDeleteEntryResponse_Result, 0, len(req.GetIds()))}

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, id := range req.GetIds() {
		id = strings.TrimSpace(id)
		if id == "" {
			resp.Results = append(resp.Results, &entryv1.BatchDeleteEntryResponse_Result{Status: apiStatus(codes.InvalidArgument, "entry ID is required")})
			continue
		}

		key, ok := a.keysByEntryID[id]
		if !ok {
			resp.Results = append(resp.Results, &entryv1.BatchDeleteEntryResponse_Result{Status: apiStatus(codes.NotFound, "entry not found"), Id: id})
			continue
		}

		delete(a.keysByEntryID, id)
		delete(a.entryIDsByKey, key)
		delete(a.entriesByID, id)
		resp.Results = append(resp.Results, &entryv1.BatchDeleteEntryResponse_Result{Status: apiStatus(codes.OK, ""), Id: id})
	}

	return resp, nil
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.TrustDomain) == "" {
		return fmt.Errorf("local spiffe: trust domain is required")
	}
	if strings.Contains(strings.TrimSpace(cfg.TrustDomain), "://") || strings.ContainsAny(strings.TrimSpace(cfg.TrustDomain), "/ \t\r\n") {
		return fmt.Errorf("local spiffe: trust domain %q must be a bare trust domain", cfg.TrustDomain)
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		return fmt.Errorf("local spiffe: data dir is required")
	}
	if strings.TrimSpace(cfg.RuntimeDir) == "" {
		return fmt.Errorf("local spiffe: runtime dir is required")
	}
	if strings.TrimSpace(cfg.WorkloadSocketPath) == "" {
		return fmt.Errorf("local spiffe: workload socket path is required")
	}
	if strings.TrimSpace(cfg.RegistrationSocketPath) == "" {
		return fmt.Errorf("local spiffe: registration socket path is required")
	}
	if cfg.WorkloadSocketPath == cfg.RegistrationSocketPath {
		return fmt.Errorf("local spiffe: workload and registration sockets must be distinct")
	}
	if strings.TrimSpace(cfg.BundleFile) == "" {
		return fmt.Errorf("local spiffe: bundle file is required")
	}
	if len(makeSelectorSet(cfg.Selectors)) == 0 {
		return fmt.Errorf("local spiffe: at least one caller selector is required")
	}
	if cfg.DefaultX509SVIDTTL < 0 {
		return fmt.Errorf("local spiffe: default X.509-SVID TTL must not be negative")
	}

	return nil
}

func (a *Authority) startServers(ctx context.Context) error {
	for _, path := range []string{a.cfg.WorkloadSocketPath, a.cfg.RegistrationSocketPath} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("local spiffe: remove stale socket %s: %w", path, err)
		}
	}

	var listenConfig net.ListenConfig
	workloadListener, err := listenConfig.Listen(ctx, "unix", a.cfg.WorkloadSocketPath)
	if err != nil {
		return fmt.Errorf("local spiffe: listen workload API socket: %w", err)
	}
	a.workloadListener = workloadListener
	_ = os.Chmod(a.cfg.WorkloadSocketPath, 0o600)

	registrationListener, err := listenConfig.Listen(ctx, "unix", a.cfg.RegistrationSocketPath)
	if err != nil {
		_ = workloadListener.Close()
		return fmt.Errorf("local spiffe: listen registration API socket: %w", err)
	}
	a.registrationListener = registrationListener
	_ = os.Chmod(a.cfg.RegistrationSocketPath, 0o600)

	a.workloadServer = grpc.NewServer()
	workloadpb.RegisterSpiffeWorkloadAPIServer(a.workloadServer, a)
	a.registrationServer = grpc.NewServer()
	entryv1.RegisterEntryServer(a.registrationServer, a)

	errCh := make(chan error, 2)
	go serveGRPC(errCh, "workload API", a.workloadServer, workloadListener)
	go serveGRPC(errCh, "registration API", a.registrationServer, registrationListener)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case <-time.After(25 * time.Millisecond):
		return nil
	}
}

func serveGRPC(errCh chan<- error, name string, server *grpc.Server, listener net.Listener) {
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		errCh <- fmt.Errorf("local spiffe: serve %s: %w", name, err)
	}
}

func ensureCA(cfg Config) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certFile := filepath.Join(cfg.DataDir, "ca.pem")
	keyFile := filepath.Join(cfg.DataDir, "ca-key.pem")

	certPEM, certErr := os.ReadFile(certFile)
	keyPEM, keyErr := os.ReadFile(keyFile)
	if certErr == nil && keyErr == nil {
		cert, key, err := parseCA(certPEM, keyPEM)
		if err != nil {
			return nil, nil, err
		}
		if cert.NotAfter.After(time.Now().Add(time.Hour)) && caMatchesTrustDomain(cert, cfg.TrustDomain) {
			return cert, key, nil
		}
	}

	if (certErr == nil) != (keyErr == nil) {
		return nil, nil, fmt.Errorf("local spiffe: CA certificate and key must either both exist or both be absent")
	}
	if certErr != nil && !os.IsNotExist(certErr) {
		return nil, nil, fmt.Errorf("local spiffe: read CA certificate: %w", certErr)
	}
	if keyErr != nil && !os.IsNotExist(keyErr) {
		return nil, nil, fmt.Errorf("local spiffe: read CA key: %w", keyErr)
	}

	cert, key, certPEM, keyPEM, err := generateCA(cfg.TrustDomain)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		return nil, nil, fmt.Errorf("local spiffe: write CA certificate: %w", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		return nil, nil, fmt.Errorf("local spiffe: write CA key: %w", err)
	}

	return cert, key, nil
}

func parseCA(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("local spiffe: CA certificate PEM is invalid")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("local spiffe: parse CA certificate: %w", err)
	}
	if !cert.IsCA {
		return nil, nil, fmt.Errorf("local spiffe: stored certificate is not a CA")
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("local spiffe: CA key PEM is invalid")
	}

	var parsed any
	switch keyBlock.Type {
	case "PRIVATE KEY":
		parsed, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	case "EC PRIVATE KEY":
		parsed, err = x509.ParseECPrivateKey(keyBlock.Bytes)
	default:
		return nil, nil, fmt.Errorf("local spiffe: unsupported CA key PEM type %q", keyBlock.Type)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("local spiffe: parse CA key: %w", err)
	}

	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("local spiffe: CA key must be ECDSA")
	}

	return cert, key, nil
}

func generateCA(trustDomain string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("local spiffe: generate CA key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	uri, _ := url.Parse("spiffe://" + trustDomain)
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Vectis Local SPIFFE Authority (" + trustDomain + ")"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		URIs:                  []*url.URL{uri},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("local spiffe: generate CA certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("local spiffe: parse generated CA certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("local spiffe: marshal CA key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return cert, key, certPEM, keyPEM, nil
}

func caMatchesTrustDomain(cert *x509.Certificate, trustDomain string) bool {
	if cert == nil {
		return false
	}

	want := "spiffe://" + strings.TrimSpace(trustDomain)
	for _, uri := range cert.URIs {
		if uri != nil && uri.String() == want {
			return true
		}
	}

	return false
}

func writeBundleFile(path string, caDER []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("local spiffe: create bundle dir: %w", err)
	}

	bundlePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err := os.WriteFile(path, bundlePEM, 0o644); err != nil {
		return fmt.Errorf("local spiffe: write bundle: %w", err)
	}

	return nil
}

func (a *Authority) prepareEntry(incoming *spiretypes.Entry, now time.Time) (*spiretypes.Entry, error) {
	entry := cloneEntry(incoming)
	if entry == nil {
		return nil, fmt.Errorf("entry is required")
	}
	if err := a.validateEntry(entry, now); err != nil {
		return nil, err
	}

	entry.Id = entryID(entry)
	if entry.GetCreatedAt() == 0 {
		entry.CreatedAt = now.Unix()
	}
	if entry.GetRevisionNumber() == 0 {
		entry.RevisionNumber = 1
	}

	return entry, nil
}

func (a *Authority) validateEntry(entry *spiretypes.Entry, now time.Time) error {
	spiffeID := entry.GetSpiffeId()
	if spiffeID == nil || strings.TrimSpace(spiffeID.GetTrustDomain()) == "" {
		return fmt.Errorf("entry SPIFFE ID is required")
	}
	if spiffeID.GetTrustDomain() != a.cfg.TrustDomain {
		return fmt.Errorf("entry SPIFFE ID trust domain %q does not match local trust domain %q", spiffeID.GetTrustDomain(), a.cfg.TrustDomain)
	}
	if strings.TrimSpace(spiffeID.GetPath()) == "" {
		return fmt.Errorf("entry SPIFFE ID path is required")
	}
	if entry.GetParentId() == nil || strings.TrimSpace(entry.GetParentId().GetTrustDomain()) == "" {
		return fmt.Errorf("entry parent SPIFFE ID is required")
	}
	if len(entry.GetSelectors()) == 0 {
		return fmt.Errorf("entry selectors are required")
	}
	for _, selector := range entry.GetSelectors() {
		if strings.TrimSpace(selector.GetType()) == "" || strings.TrimSpace(selector.GetValue()) == "" {
			return fmt.Errorf("entry selectors must include type and value")
		}
	}
	if entry.GetExpiresAt() > 0 && entry.GetExpiresAt() <= now.Unix() {
		return fmt.Errorf("entry is expired")
	}
	if entry.GetX509SvidTtl() < 0 {
		return fmt.Errorf("entry X.509-SVID TTL must not be negative")
	}

	return nil
}

func applyEntryUpdate(existing, incoming *spiretypes.Entry, mask *spiretypes.EntryMask) {
	if mask == nil {
		replacement := cloneEntry(incoming)
		if replacement != nil {
			proto.Reset(existing)
			proto.Merge(existing, replacement)
		}

		return
	}

	if mask.GetSpiffeId() {
		existing.SpiffeId = cloneSPIFFEID(incoming.GetSpiffeId())
	}
	if mask.GetParentId() {
		existing.ParentId = cloneSPIFFEID(incoming.GetParentId())
	}
	if mask.GetSelectors() {
		existing.Selectors = cloneSelectors(incoming.GetSelectors())
	}
	if mask.GetX509SvidTtl() {
		existing.X509SvidTtl = incoming.GetX509SvidTtl()
	}
	if mask.GetExpiresAt() {
		existing.ExpiresAt = incoming.GetExpiresAt()
	}
	if mask.GetHint() {
		existing.Hint = incoming.GetHint()
	}
}

func (a *Authority) matchingEntries(now time.Time) []*spiretypes.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.purgeExpiredLocked(now)
	entries := make([]*spiretypes.Entry, 0, len(a.entriesByID))
	for _, entry := range a.entriesByID {
		if a.matchesCallerSelectors(entry) {
			entries = append(entries, cloneEntry(entry))
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].GetSpiffeId().GetPath() < entries[j].GetSpiffeId().GetPath()
	})

	return entries
}

func (a *Authority) matchesCallerSelectors(entry *spiretypes.Entry) bool {
	if len(entry.GetSelectors()) == 0 {
		return false
	}
	for _, selector := range entry.GetSelectors() {
		if _, ok := a.callerSelectorSet[selectorKey(selector)]; !ok {
			return false
		}
	}

	return true
}

func (a *Authority) purgeExpiredLocked(now time.Time) {
	for id, entry := range a.entriesByID {
		if entry.GetExpiresAt() > 0 && entry.GetExpiresAt() <= now.Unix() {
			key := a.keysByEntryID[id]
			delete(a.entriesByID, id)
			delete(a.keysByEntryID, id)
			delete(a.entryIDsByKey, key)
		}
	}
}

func (a *Authority) signX509SVID(entry *spiretypes.Entry, now time.Time) (*workloadpb.X509SVID, error) {
	spiffeID := spiffeIDString(entry.GetSpiffeId())
	uri, err := url.Parse(spiffeID)
	if err != nil {
		return nil, fmt.Errorf("parse SPIFFE ID: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate SVID key: %w", err)
	}

	ttl := a.cfg.DefaultX509SVIDTTL
	if entry.GetX509SvidTtl() > 0 {
		ttl = time.Duration(entry.GetX509SvidTtl()) * time.Second
	}
	notAfter := now.Add(ttl)
	if entry.GetExpiresAt() > 0 {
		entryExpiry := time.Unix(entry.GetExpiresAt(), 0).UTC()
		if entryExpiry.Before(notAfter) {
			notAfter = entryExpiry
		}
	}
	if !notAfter.After(now) {
		return nil, fmt.Errorf("entry is expired")
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: spiffeID,
		},
		NotBefore:   now.Add(-time.Minute),
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{uri},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, a.caCert, key.Public(), a.caKey)
	if err != nil {
		return nil, fmt.Errorf("sign SVID certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal SVID key: %w", err)
	}

	return &workloadpb.X509SVID{
		SpiffeId:    spiffeID,
		X509Svid:    certDER,
		X509SvidKey: keyDER,
		Bundle:      a.caCert.Raw,
		Hint:        entry.GetHint(),
	}, nil
}

func entryID(entry *spiretypes.Entry) string {
	sum := sha256.Sum256([]byte(entryKey(entry)))
	return "local-" + hex.EncodeToString(sum[:12])
}

func entryKey(entry *spiretypes.Entry) string {
	selectors := make([]string, 0, len(entry.GetSelectors()))
	for _, selector := range entry.GetSelectors() {
		selectors = append(selectors, selectorKey(selector))
	}
	sort.Strings(selectors)

	return strings.Join([]string{
		spiffeIDString(entry.GetSpiffeId()),
		spiffeIDString(entry.GetParentId()),
		strings.Join(selectors, ","),
	}, "\x00")
}

func makeSelectorSet(selectors []string) map[string]struct{} {
	out := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		typ, value, ok := strings.Cut(strings.TrimSpace(selector), ":")
		if !ok || strings.TrimSpace(typ) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		out[strings.TrimSpace(typ)+":"+strings.TrimSpace(value)] = struct{}{}
	}

	return out
}

func selectorKey(selector *spiretypes.Selector) string {
	if selector == nil {
		return ""
	}

	return strings.TrimSpace(selector.GetType()) + ":" + strings.TrimSpace(selector.GetValue())
}

func spiffeIDString(id *spiretypes.SPIFFEID) string {
	if id == nil {
		return ""
	}

	path := strings.TrimSpace(id.GetPath())
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return "spiffe://" + strings.TrimSpace(id.GetTrustDomain()) + path
}

func cloneEntry(entry *spiretypes.Entry) *spiretypes.Entry {
	if entry == nil {
		return nil
	}

	return proto.Clone(entry).(*spiretypes.Entry)
}

func cloneSPIFFEID(id *spiretypes.SPIFFEID) *spiretypes.SPIFFEID {
	if id == nil {
		return nil
	}

	return proto.Clone(id).(*spiretypes.SPIFFEID)
}

func cloneSelectors(selectors []*spiretypes.Selector) []*spiretypes.Selector {
	if len(selectors) == 0 {
		return nil
	}

	out := make([]*spiretypes.Selector, 0, len(selectors))
	for _, selector := range selectors {
		if selector == nil {
			continue
		}
		out = append(out, proto.Clone(selector).(*spiretypes.Selector))
	}

	return out
}

func apiStatus(code codes.Code, message string) *spiretypes.Status {
	return &spiretypes.Status{Code: int32(code), Message: message}
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("local spiffe: generate serial: %w", err)
	}

	return serial, nil
}

package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sdkartifact "vectis/sdk/artifact"
)

func TestStorePutStatAndOpen(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	store := newTestStore(t, fake.server.URL)
	desc, err := store.Put(context.Background(), strings.NewReader("hello s3"), sdkartifact.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantDigest := sha256Hex("hello s3")
	if desc != sdkartifact.DescriptorForSHA256(wantDigest, int64(len("hello s3"))) {
		t.Fatalf("descriptor = %+v", desc)
	}

	putAuthorization := fake.lastHeader("Authorization")
	if !strings.Contains(putAuthorization, "Credential=AKIA_TEST/20260102/us-west-2/s3/aws4_request") {
		t.Fatalf("PUT Authorization header = %q", putAuthorization)
	}

	for _, wantSignedHeader := range []string{
		"x-amz-content-sha256",
		"x-amz-date",
		"x-amz-meta-vectis-algorithm",
		"x-amz-meta-vectis-digest",
	} {
		if !strings.Contains(putAuthorization, wantSignedHeader) {
			t.Fatalf("PUT Authorization header missing signed header %q: %q", wantSignedHeader, putAuthorization)
		}
	}

	stat, err := store.Stat(context.Background(), desc.Key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if stat != desc {
		t.Fatalf("stat = %+v, want %+v", stat, desc)
	}

	openDesc, rc, err := store.Open(context.Background(), desc.Key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	if openDesc != desc {
		t.Fatalf("open descriptor = %+v, want %+v", openDesc, desc)
	}

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read open body: %v", err)
	}

	if string(body) != "hello s3" {
		t.Fatalf("body = %q", body)
	}

	wantKey := "prefix/blobs/sha256/" + wantDigest[:2] + "/" + wantDigest[2:4] + "/" + wantDigest + ".blob"
	if !fake.hasKey(wantKey) {
		t.Fatalf("fake s3 missing object key %q", wantKey)
	}
}

func TestStorePutExpectedDigestUsesExistingObject(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	store := newTestStore(t, fake.server.URL)
	first, err := store.Put(context.Background(), strings.NewReader("same"), sdkartifact.PutOptions{})
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}

	putCount := fake.methodCount(http.MethodPut)
	second, err := store.Put(context.Background(), strings.NewReader("not-read"), sdkartifact.PutOptions{
		ExpectedSHA256: first.Digest,
		ExpectedSize:   first.Size,
		RequireSize:    true,
	})

	if err != nil {
		t.Fatalf("Put existing: %v", err)
	}

	if second != first {
		t.Fatalf("second = %+v, want %+v", second, first)
	}

	if got := fake.methodCount(http.MethodPut); got != putCount {
		t.Fatalf("PUT count = %d, want %d", got, putCount)
	}
}

func TestStorePutRejectsExpectedDigestMismatch(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	store := newTestStore(t, fake.server.URL)
	_, err := store.Put(context.Background(), strings.NewReader("payload"), sdkartifact.PutOptions{
		ExpectedSHA256: sha256Hex("other"),
	})

	if err == nil {
		t.Fatal("expected digest mismatch")
	}

	if !strings.Contains(err.Error(), sdkartifact.ErrBlobDigestMismatch.Error()) {
		t.Fatalf("error = %v, want digest mismatch", err)
	}

	if got := fake.methodCount(http.MethodPut); got != 0 {
		t.Fatalf("PUT count = %d, want 0", got)
	}
}

func TestStoreStatsListsBlobPrefix(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	store := newTestStore(t, fake.server.URL)
	first, err := store.Put(context.Background(), strings.NewReader("first"), sdkartifact.PutOptions{})
	if err != nil {
		t.Fatalf("Put first: %v", err)
	}

	second, err := store.Put(context.Background(), strings.NewReader("second"), sdkartifact.PutOptions{})
	if err != nil {
		t.Fatalf("Put second: %v", err)
	}

	stats, err := store.StorageStats(context.Background())
	if err != nil {
		t.Fatalf("StorageStats: %v", err)
	}

	if stats.BlobFiles != 2 || stats.BlobBytes != first.Size+second.Size || !stats.NewBlobWritable {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestStoreReturnsNotFound(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	store := newTestStore(t, fake.server.URL)
	_, err := store.Stat(context.Background(), sdkartifact.BlobKeySHA256(sha256Hex("missing")))
	if err == nil || !strings.Contains(err.Error(), sdkartifact.ErrBlobNotFound.Error()) {
		t.Fatalf("Stat error = %v, want not found", err)
	}
}

func TestStorePutStatAndOpenUnsigned(t *testing.T) {
	fake := newFakeS3(t)
	fake.requireAuthorization = false
	defer fake.server.Close()

	store, err := NewStore(StoreOptions{
		Endpoint:  fake.server.URL,
		Region:    "us-west-2",
		Bucket:    "vectis-artifacts",
		Prefix:    "public",
		PathStyle: true,
	})

	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	desc, err := store.Put(context.Background(), strings.NewReader("public s3"), sdkartifact.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if fake.lastHeader("Authorization") != "" {
		t.Fatalf("unsigned store sent Authorization header %q", fake.lastHeader("Authorization"))
	}

	if _, err := store.Stat(context.Background(), desc.Key); err != nil {
		t.Fatalf("Stat: %v", err)
	}

	_, rc, err := store.Open(context.Background(), desc.Key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read open body: %v", err)
	}

	if string(body) != "public s3" {
		t.Fatalf("body = %q", body)
	}
}

func TestStoreRequiresAccessKeyIDAndSecretAccessKeyTogether(t *testing.T) {
	for _, tt := range []struct {
		name            string
		accessKeyID     string
		secretAccessKey string
	}{
		{name: "access key without secret", accessKeyID: "AKIA_TEST"},
		{name: "secret without access key", secretAccessKey: "SECRET_TEST"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeS3(t)
			defer fake.server.Close()

			store, err := NewStore(StoreOptions{
				Endpoint:        fake.server.URL,
				Region:          "us-west-2",
				Bucket:          "vectis-artifacts",
				Prefix:          "prefix",
				AccessKeyID:     tt.accessKeyID,
				SecretAccessKey: tt.secretAccessKey,
				PathStyle:       true,
			})

			if err != nil {
				t.Fatalf("NewStore: %v", err)
			}

			_, err = store.Stat(context.Background(), sdkartifact.BlobKeySHA256(sha256Hex("missing")))
			if err == nil || !strings.Contains(err.Error(), "must be configured together") {
				t.Fatalf("Stat error = %v, want credential pair validation", err)
			}

			if got := fake.methodCount(http.MethodHead); got != 0 {
				t.Fatalf("HEAD count = %d, want request to fail before reaching endpoint", got)
			}
		})
	}
}

func newTestStore(t *testing.T, endpoint string) *Store {
	t.Helper()

	store, err := NewStore(StoreOptions{
		Endpoint:        endpoint,
		Region:          "us-west-2",
		Bucket:          "vectis-artifacts",
		Prefix:          "prefix",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "SECRET_TEST",
		PathStyle:       true,
		now: func() time.Time {
			return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		},
	})

	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	return store
}

type fakeS3 struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	methods map[string]int
	headers http.Header

	requireAuthorization bool
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()

	f := &fakeS3{
		t:                    t,
		objects:              map[string][]byte{},
		methods:              map[string]int{},
		headers:              http.Header{},
		requireAuthorization: true,
	}

	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	return f
}

func (f *fakeS3) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.methods[r.Method]++
	f.headers = r.Header.Clone()
	f.mu.Unlock()

	if f.requireAuthorization && r.Header.Get("Authorization") == "" {
		http.Error(w, "missing authorization", http.StatusForbidden)
		return
	}

	if f.requireAuthorization && (r.Header.Get("x-amz-content-sha256") == "" || r.Header.Get("x-amz-date") == "") {
		http.Error(w, "missing sigv4 headers", http.StatusForbidden)
		return
	}

	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		f.serveList(w, r)
		return
	}

	key := fakeS3ObjectKey(f.t, r.URL)
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		f.objects[key] = data
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		data, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		data, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data)
	default:
		http.Error(w, "unsupported", http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) serveList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	result := struct {
		XMLName     xml.Name `xml:"ListBucketResult"`
		IsTruncated bool     `xml:"IsTruncated"`
		Contents    []struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
		} `xml:"Contents"`
	}{}

	for _, key := range keys {
		result.Contents = append(result.Contents, struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
		}{Key: key, Size: int64(len(f.objects[key]))})
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(result); err != nil {
		f.t.Errorf("encode list: %v", err)
	}
}

func fakeS3ObjectKey(t *testing.T, u *url.URL) string {
	t.Helper()

	parts := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if len(parts) < 2 || parts[0] != "vectis-artifacts" {
		t.Fatalf("unexpected path-style object path %q", u.EscapedPath())
	}

	unescaped := make([]string, 0, len(parts)-1)
	for _, part := range parts[1:] {
		value, err := url.PathUnescape(part)
		if err != nil {
			t.Fatalf("unescape path part %q: %v", part, err)
		}

		unescaped = append(unescaped, value)
	}

	return strings.Join(unescaped, "/")
}

func (f *fakeS3) hasKey(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.objects[key]
	return ok
}

func (f *fakeS3) lastHeader(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headers.Get(name)
}

func (f *fakeS3) methodCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.methods[method]
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

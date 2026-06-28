package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestArtifactService_UploadStatAndReadBlob(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	client := newArtifactServiceClient(t, store, ServerOptions{ReadBlobChunkBytes: 4})
	ctx := context.Background()
	wantData := []byte("hello artifact")
	wantDigest := sha256BytesHex(wantData)

	upload, err := client.UploadBlob(ctx)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}

	if err := upload.Send(&api.UploadBlobRequest{
		Data:           []byte("hello "),
		ExpectedSha256: strPtr(wantDigest),
		ExpectedSize:   int64Ptr(int64(len(wantData))),
		RequireSize:    boolPtr(true),
	}); err != nil {
		t.Fatalf("send first upload chunk: %v", err)
	}

	if err := upload.Send(&api.UploadBlobRequest{Data: []byte("artifact")}); err != nil {
		t.Fatalf("send second upload chunk: %v", err)
	}

	desc, err := upload.CloseAndRecv()
	if err != nil {
		t.Fatalf("close upload: %v", err)
	}

	assertAPIDescriptor(t, desc, wantDigest, int64(len(wantData)))

	stat, err := client.StatBlob(ctx, &api.GetBlobRequest{Key: desc.Key})
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}

	assertAPIDescriptor(t, stat, wantDigest, int64(len(wantData)))

	read, err := client.ReadBlob(ctx, &api.GetBlobRequest{Key: desc.Key})
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}

	var got bytes.Buffer
	var sawDescriptor bool
	for {
		resp, err := read.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			t.Fatalf("receive read chunk: %v", err)
		}

		if resp.GetBlob() != nil {
			if sawDescriptor {
				t.Fatal("read stream sent descriptor more than once")
			}

			sawDescriptor = true
			assertAPIDescriptor(t, resp.GetBlob(), wantDigest, int64(len(wantData)))
		}

		got.Write(resp.GetData())
	}

	if !sawDescriptor {
		t.Fatal("read stream did not send descriptor")
	}

	if !bytes.Equal(got.Bytes(), wantData) {
		t.Fatalf("read data = %q, want %q", got.Bytes(), wantData)
	}
}

func TestArtifactService_UploadEmptyBlobAndReadDescriptor(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	client := newArtifactServiceClient(t, store, ServerOptions{})
	ctx := context.Background()
	upload, err := client.UploadBlob(ctx)
	if err != nil {
		t.Fatalf("start upload: %v", err)
	}

	desc, err := upload.CloseAndRecv()
	if err != nil {
		t.Fatalf("close empty upload: %v", err)
	}

	assertAPIDescriptor(t, desc, sha256BytesHex(nil), 0)

	read, err := client.ReadBlob(ctx, &api.GetBlobRequest{Key: desc.Key})
	if err != nil {
		t.Fatalf("read empty blob: %v", err)
	}

	resp, err := read.Recv()
	if err != nil {
		t.Fatalf("receive empty blob descriptor: %v", err)
	}

	assertAPIDescriptor(t, resp.GetBlob(), sha256BytesHex(nil), 0)
	if len(resp.GetData()) != 0 {
		t.Fatalf("empty blob read returned data %q", resp.GetData())
	}

	if _, err := read.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected read EOF, got %v", err)
	}
}

func TestArtifactService_StatusMapping(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	client := newArtifactServiceClient(t, store, ServerOptions{})
	ctx := context.Background()

	_, err = client.StatBlob(ctx, &api.GetBlobRequest{Key: strPtr(BlobKeySHA256(stringOfByte('0', sha256.Size*2)))})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing stat code = %v, want %v; err=%v", status.Code(err), codes.NotFound, err)
	}

	_, err = client.StatBlob(ctx, &api.GetBlobRequest{Key: strPtr("not-a-blob-key")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid key code = %v, want %v; err=%v", status.Code(err), codes.InvalidArgument, err)
	}

	upload, err := client.UploadBlob(ctx)
	if err != nil {
		t.Fatalf("start oversized upload: %v", err)
	}

	if err := upload.Send(&api.UploadBlobRequest{
		Data:     []byte("too large"),
		MaxBytes: int64Ptr(3),
	}); err != nil {
		t.Fatalf("send oversized upload: %v", err)
	}

	_, err = upload.CloseAndRecv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized upload code = %v, want %v; err=%v", status.Code(err), codes.ResourceExhausted, err)
	}
}

func newArtifactServiceClient(t *testing.T, store Store, opts ServerOptions) api.ArtifactServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	api.RegisterArtifactServiceServer(srv, NewServerWithOptions(store, opts))

	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("artifact test gRPC server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///artifact-bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("new artifact client: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return api.NewArtifactServiceClient(conn)
}

func assertAPIDescriptor(t *testing.T, desc *api.BlobDescriptor, digest string, size int64) {
	t.Helper()

	if desc == nil {
		t.Fatal("descriptor is nil")
	}

	if desc.GetKey() != BlobKeySHA256(digest) {
		t.Fatalf("key = %q, want %q", desc.GetKey(), BlobKeySHA256(digest))
	}

	if desc.GetAlgorithm() != HashSHA256 {
		t.Fatalf("algorithm = %q, want %q", desc.GetAlgorithm(), HashSHA256)
	}

	if desc.GetDigest() != digest {
		t.Fatalf("digest = %q, want %q", desc.GetDigest(), digest)
	}

	if desc.GetSize() != size {
		t.Fatalf("size = %d, want %d", desc.GetSize(), size)
	}
}

func sha256BytesHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func strPtr(s string) *string {
	return &s
}

func int64Ptr(n int64) *int64 {
	return &n
}

func boolPtr(v bool) *bool {
	return &v
}

func stringOfByte(b byte, n int) string {
	return string(bytes.Repeat([]byte{b}, n))
}

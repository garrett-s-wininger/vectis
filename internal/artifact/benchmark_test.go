package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const benchmarkArtifactBufSize = 16 * 1024 * 1024

func BenchmarkArtifact_CopyHashingDiscard(b *testing.B) {
	for _, size := range benchmarkArtifactSizes() {
		b.Run(benchmarkArtifactSizeName(size), func(b *testing.B) {
			payload := benchmarkArtifactPayload(size)
			ctx := context.Background()

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := copyHashing(ctx, io.Discard, bytes.NewReader(payload), sha256.New(), 0); err != nil {
					b.Fatalf("copy hashing artifact payload: %v", err)
				}
			}
		})
	}
}

func BenchmarkLocalStore_Put(b *testing.B) {
	for _, size := range benchmarkArtifactSizes() {
		b.Run(benchmarkArtifactSizeName(size), func(b *testing.B) {
			store, err := NewLocalStore(b.TempDir())
			if err != nil {
				b.Fatalf("new local store: %v", err)
			}
			defer store.Close()

			payload := benchmarkArtifactPayload(size)
			ctx := context.Background()

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desc, err := store.Put(ctx, bytes.NewReader(payload), PutOptions{
					ExpectedSize: int64(size),
					RequireSize:  true,
				})
				if err != nil {
					b.Fatalf("put artifact payload: %v", err)
				}

				b.StopTimer()
				removeBenchmarkArtifactBlob(b, store, desc.Digest)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkLocalStore_OpenRead(b *testing.B) {
	for _, size := range benchmarkArtifactSizes() {
		b.Run(benchmarkArtifactSizeName(size), func(b *testing.B) {
			store, err := NewLocalStore(b.TempDir())
			if err != nil {
				b.Fatalf("new local store: %v", err)
			}
			defer store.Close()

			payload := benchmarkArtifactPayload(size)
			ctx := context.Background()
			desc, err := store.Put(ctx, bytes.NewReader(payload), PutOptions{})
			if err != nil {
				b.Fatalf("seed artifact payload: %v", err)
			}

			buf := make([]byte, defaultReadBlobChunkBytes)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, rc, err := store.Open(ctx, desc.Key)
				if err != nil {
					b.Fatalf("open artifact payload: %v", err)
				}

				n, readErr := readBenchmarkArtifactBlob(rc, buf)
				closeErr := rc.Close()
				if readErr != nil {
					b.Fatalf("read artifact payload: %v", readErr)
				}

				if closeErr != nil {
					b.Fatalf("close artifact payload: %v", closeErr)
				}

				if n != int64(size) {
					b.Fatalf("read %d bytes, want %d", n, size)
				}
			}
		})
	}
}

func BenchmarkArtifactService_UploadBlob(b *testing.B) {
	for _, size := range benchmarkArtifactSizes() {
		for _, chunkBytes := range benchmarkArtifactChunkSizes() {
			b.Run(fmt.Sprintf("%s/chunk_%s", benchmarkArtifactSizeName(size), benchmarkArtifactSizeName(chunkBytes)), func(b *testing.B) {
				store, err := NewLocalStore(b.TempDir())
				if err != nil {
					b.Fatalf("new local store: %v", err)
				}
				defer store.Close()

				client := newBenchmarkArtifactServiceClient(b, store, ServerOptions{})
				payload := benchmarkArtifactPayload(size)
				ctx := context.Background()

				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					desc, err := uploadBenchmarkArtifactBlob(ctx, client, payload, chunkBytes)
					if err != nil {
						b.Fatalf("upload artifact payload: %v", err)
					}

					b.StopTimer()
					removeBenchmarkArtifactBlob(b, store, desc.GetDigest())
					b.StartTimer()
				}
			})
		}
	}
}

func BenchmarkArtifactService_ReadBlob(b *testing.B) {
	for _, size := range benchmarkArtifactSizes() {
		for _, chunkBytes := range benchmarkArtifactChunkSizes() {
			b.Run(fmt.Sprintf("%s/chunk_%s", benchmarkArtifactSizeName(size), benchmarkArtifactSizeName(chunkBytes)), func(b *testing.B) {
				store, err := NewLocalStore(b.TempDir())
				if err != nil {
					b.Fatalf("new local store: %v", err)
				}
				defer store.Close()

				client := newBenchmarkArtifactServiceClient(b, store, ServerOptions{ReadBlobChunkBytes: chunkBytes})
				payload := benchmarkArtifactPayload(size)
				ctx := context.Background()
				desc, err := store.Put(ctx, bytes.NewReader(payload), PutOptions{})
				if err != nil {
					b.Fatalf("seed artifact payload: %v", err)
				}

				key := desc.Key
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					n, err := readBenchmarkArtifactServiceBlob(ctx, client, key)
					if err != nil {
						b.Fatalf("read artifact service payload: %v", err)
					}

					if n != int64(size) {
						b.Fatalf("read %d bytes, want %d", n, size)
					}
				}
			})
		}
	}
}

func benchmarkArtifactSizes() []int {
	return []int{64 << 10, 1 << 20, 8 << 20}
}

func benchmarkArtifactChunkSizes() []int {
	return []int{defaultReadBlobChunkBytes, 1 << 20}
}

func benchmarkArtifactSizeName(size int) string {
	switch {
	case size >= 1<<20 && size%(1<<20) == 0:
		return fmt.Sprintf("%02dMiB", size/(1<<20))
	case size >= 1<<10 && size%(1<<10) == 0:
		return fmt.Sprintf("%03dKiB", size/(1<<10))
	default:
		return fmt.Sprintf("%dB", size)
	}
}

func benchmarkArtifactPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*31 + i/251)
	}

	return payload
}

func removeBenchmarkArtifactBlob(b *testing.B, store *LocalStore, digest string) {
	b.Helper()

	if err := os.Remove(store.sha256Path(digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
		b.Fatalf("remove benchmark artifact blob: %v", err)
	}
}

func readBenchmarkArtifactBlob(r io.Reader, buf []byte) (int64, error) {
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			total += int64(n)
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}

			return total, err
		}
	}
}

func uploadBenchmarkArtifactBlob(ctx context.Context, client api.ArtifactServiceClient, payload []byte, chunkBytes int) (*api.BlobDescriptor, error) {
	stream, err := client.UploadBlob(ctx)
	if err != nil {
		return nil, err
	}

	expectedSize := int64(len(payload))
	requireSize := true
	for offset := 0; offset < len(payload); offset += chunkBytes {
		end := offset + chunkBytes
		if end > len(payload) {
			end = len(payload)
		}

		req := &api.UploadBlobRequest{Data: payload[offset:end]}
		if offset == 0 {
			req.ExpectedSize = &expectedSize
			req.RequireSize = &requireSize
		}

		if err := stream.Send(req); err != nil {
			return nil, err
		}
	}

	return stream.CloseAndRecv()
}

func readBenchmarkArtifactServiceBlob(ctx context.Context, client api.ArtifactServiceClient, key string) (int64, error) {
	stream, err := client.ReadBlob(ctx, &api.GetBlobRequest{Key: &key})
	if err != nil {
		return 0, err
	}

	var total int64
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}

			return total, err
		}

		total += int64(len(resp.GetData()))
	}
}

func newBenchmarkArtifactServiceClient(b *testing.B, store Store, opts ServerOptions) api.ArtifactServiceClient {
	b.Helper()

	lis := bufconn.Listen(benchmarkArtifactBufSize)
	srv := grpc.NewServer()
	api.RegisterArtifactServiceServer(srv, NewServerWithOptions(store, opts))

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			b.Logf("artifact benchmark gRPC server error: %v", err)
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
		b.Fatalf("new artifact client: %v", err)
	}

	b.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return api.NewArtifactServiceClient(conn)
}

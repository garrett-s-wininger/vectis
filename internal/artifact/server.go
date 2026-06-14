package artifact

import (
	"bytes"
	"context"
	"errors"
	"io"

	api "vectis/api/gen/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultReadBlobChunkBytes = defaultArtifactChunkBytes

type Store interface {
	Put(context.Context, io.Reader, PutOptions) (BlobDescriptor, error)
	Stat(context.Context, string) (BlobDescriptor, error)
	Open(context.Context, string) (BlobDescriptor, io.ReadCloser, error)
}

type ServerOptions struct {
	ReadBlobChunkBytes int
}

type Server struct {
	api.UnimplementedArtifactServiceServer
	store          Store
	readChunkBytes int
}

func NewServer(store Store) *Server {
	return NewServerWithOptions(store, ServerOptions{})
}

func NewServerWithOptions(store Store, opts ServerOptions) *Server {
	chunkBytes := opts.ReadBlobChunkBytes
	if chunkBytes <= 0 {
		chunkBytes = defaultReadBlobChunkBytes
	}

	return &Server{
		store:          store,
		readChunkBytes: chunkBytes,
	}
}

func (s *Server) UploadBlob(stream api.ArtifactService_UploadBlobServer) error {
	first, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	var r io.Reader
	var opts PutOptions
	if errors.Is(err, io.EOF) {
		r = bytes.NewReader(nil)
	} else {
		opts = putOptionsFromUploadRequest(first)
		r = &uploadBlobStreamReader{
			stream: stream,
			buf:    first.GetData(),
		}
	}

	desc, err := s.store.Put(stream.Context(), r, opts)
	if err != nil {
		return mapArtifactError(err)
	}

	return stream.SendAndClose(toAPIBlobDescriptor(desc))
}

func putOptionsFromUploadRequest(req *api.UploadBlobRequest) PutOptions {
	if req == nil {
		return PutOptions{}
	}

	return PutOptions{
		ExpectedSHA256: req.GetExpectedSha256(),
		ExpectedSize:   req.GetExpectedSize(),
		RequireSize:    req.GetRequireSize(),
		MaxBytes:       req.GetMaxBytes(),
	}
}

type uploadBlobStreamReader struct {
	stream api.ArtifactService_UploadBlobServer
	buf    []byte
	eof    bool
}

func (r *uploadBlobStreamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for len(r.buf) == 0 && !r.eof {
		req, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.eof = true
			break
		}

		if err != nil {
			return 0, err
		}

		r.buf = req.GetData()
	}

	if len(r.buf) == 0 && r.eof {
		return 0, io.EOF
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (s *Server) StatBlob(ctx context.Context, req *api.GetBlobRequest) (*api.BlobDescriptor, error) {
	desc, err := s.store.Stat(ctx, req.GetKey())
	if err != nil {
		return nil, mapArtifactError(err)
	}

	return toAPIBlobDescriptor(desc), nil
}

func (s *Server) ReadBlob(req *api.GetBlobRequest, stream api.ArtifactService_ReadBlobServer) error {
	ctx := stream.Context()
	desc, rc, err := s.store.Open(ctx, req.GetKey())
	if err != nil {
		return mapArtifactError(err)
	}
	defer rc.Close()

	buf, releaseBuf := borrowArtifactBuffer(s.readChunkBytes)
	defer releaseBuf()

	sentDescriptor := false
	for {
		if err := ctx.Err(); err != nil {
			return mapArtifactError(err)
		}

		n, readErr := rc.Read(buf)
		if n > 0 {
			resp := &api.ReadBlobResponse{
				Data: append([]byte(nil), buf[:n]...),
			}

			if !sentDescriptor {
				resp.Blob = toAPIBlobDescriptor(desc)
				sentDescriptor = true
			}

			if err := stream.Send(resp); err != nil {
				return err
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}

			return status.Errorf(codes.Internal, "read artifact blob %s: %v", desc.Key, readErr)
		}
	}

	if !sentDescriptor {
		return stream.Send(&api.ReadBlobResponse{Blob: toAPIBlobDescriptor(desc)})
	}

	return nil
}

func toAPIBlobDescriptor(desc BlobDescriptor) *api.BlobDescriptor {
	return &api.BlobDescriptor{
		Key:       &desc.Key,
		Algorithm: &desc.Algorithm,
		Digest:    &desc.Digest,
		Size:      &desc.Size,
	}
}

func mapArtifactError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}

	if errors.Is(err, ErrBlobNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}

	if errors.Is(err, ErrStoreReadOnly) || errors.Is(err, ErrBlobTooLarge) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}

	if errors.Is(err, ErrBlobDigestMismatch) ||
		errors.Is(err, ErrBlobSizeMismatch) ||
		errors.Is(err, ErrInvalidBlobDescriptor) ||
		errors.Is(err, ErrInvalidBlobKey) ||
		errors.Is(err, ErrInvalidDigest) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return status.Error(codes.Internal, err.Error())
}

package logforwarder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const maxChunkSize = 16 * 1024 * 1024 // 16 MiB

// SocketServer listens on a Unix domain socket and accepts LogChunk streams
// from workers.
type SocketServer struct {
	socketPath string
	listener   net.Listener
	chunks     chan *api.LogChunk
	wg         sync.WaitGroup
	closeOnce  sync.Once
	logger     Logger
	mu         sync.Mutex
	conns      map[net.Conn]struct{}
}

// Logger is the minimal logging interface used by SocketServer.
type Logger interface {
	Warn(msg string, args ...any)
}

// NewSocketServer creates a server bound to the given Unix socket path.
func NewSocketServer(socketPath string, bufferSize int) (*SocketServer, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("bufferSize must be positive")
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on unix socket: %w", err)
	}

	if err := os.Chmod(socketPath, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}

	return &SocketServer{
		socketPath: socketPath,
		listener:   ln,
		chunks:     make(chan *api.LogChunk, bufferSize),
		conns:      make(map[net.Conn]struct{}),
	}, nil
}

// SetLogger configures the logger used for warnings (e.g. dropped chunks).
func (s *SocketServer) SetLogger(l Logger) {
	s.logger = l
}

// Chunks returns the channel that receives decoded LogChunks from connected
// workers.
func (s *SocketServer) Chunks() <-chan *api.LogChunk {
	return s.chunks
}

// Serve accepts connections until the listener is closed.
func (s *SocketServer) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // listener closed
			}

			time.Sleep(5 * time.Millisecond)
			continue
		}

		s.wg.Add(1)
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

// Close stops the listener, closes all active connections, waits for handlers
// to finish, and then closes the chunk channel. It is safe to call multiple
// times.
func (s *SocketServer) Close() error {
	s.closeOnce.Do(func() {
		if s.listener != nil {
			s.listener.Close()
		}

		s.mu.Lock()
		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()

		s.wg.Wait()
		close(s.chunks)
		os.Remove(s.socketPath)
	})
	return nil
}

func (s *SocketServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	for {
		chunk, err := readChunk(conn)
		if err != nil {
			if err == io.EOF {
				return
			}

			if s.logger != nil {
				s.logger.Warn("Socket protocol error: %v", err)
			}
			return
		}

		select {
		case s.chunks <- chunk:
		default:
			if s.logger != nil {
				s.logger.Warn("Socket buffer full; dropping chunk (run %s seq %d)", chunk.GetRunId(), chunk.GetSequence())
			}
		}
	}
}

func readChunk(r io.Reader) (*api.LogChunk, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	if length > maxChunkSize {
		return nil, fmt.Errorf("chunk size %d exceeds max %d", length, maxChunkSize)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	var chunk api.LogChunk
	if err := proto.Unmarshal(data, &chunk); err != nil {
		return nil, fmt.Errorf("unmarshal chunk: %w", err)
	}

	return &chunk, nil
}

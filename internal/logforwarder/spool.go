package logforwarder

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const (
	spoolMagic         = "VLOG"
	spoolVersion       = 1
	spoolExt           = ".spool"
	spoolTmpExt        = ".spool.tmp"
	maxSpoolBatchCount = 100000
)

// SpoolWriter writes batches of LogChunks to a spool file.
type SpoolWriter struct {
	file    *os.File
	chunks  []*api.LogChunk
	maxSize int
}

// NewSpoolWriter creates a writer for a new spool file.
func NewSpoolWriter(path string, maxSize int) (*SpoolWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}

	return &SpoolWriter{
		file:    f,
		maxSize: maxSize,
	}, nil
}

// Append adds a chunk to the in-memory buffer.
func (w *SpoolWriter) Append(chunk *api.LogChunk) error {
	if chunk == nil {
		return fmt.Errorf("cannot append nil chunk")
	}

	if len(w.chunks) >= w.maxSize {
		return fmt.Errorf("spool batch full (%d chunks)", w.maxSize)
	}

	w.chunks = append(w.chunks, chunk)
	return nil
}

// flush serializes the buffered chunks and writes the complete batch to disk.
// It must be called at most once per SpoolWriter because the file format
// includes a single header that is written by encodeBatch.
func (w *SpoolWriter) flush() error {
	if len(w.chunks) == 0 {
		return nil
	}

	payload, err := encodeBatch(w.chunks)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}

	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync spool: %w", err)
	}

	w.chunks = w.chunks[:0]
	return nil
}

// Close flushes any remaining chunks and closes the file.
func (w *SpoolWriter) Close() error {
	if err := w.flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("flush spool: %w", err)
	}

	return w.file.Close()
}

// SpoolReader reads batches from a spool file.
type SpoolReader struct {
	file   *os.File
	reader *bufio.Reader
}

// NewSpoolReader opens a spool file for reading.
func NewSpoolReader(path string) (*SpoolReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}

	// Validate header
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		f.Close()
		return nil, fmt.Errorf("read magic: %w", err)
	}

	if string(magic) != spoolMagic {
		f.Close()
		return nil, fmt.Errorf("invalid spool magic: %q", magic)
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		f.Close()
		return nil, fmt.Errorf("read version: %w", err)
	}

	if version != spoolVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported spool version: %d", version)
	}

	return &SpoolReader{file: f, reader: bufio.NewReader(f)}, nil
}

// ReadBatch reads the next batch of chunks from the spool file.
// Returns io.EOF when there are no more batches.
func (r *SpoolReader) ReadBatch() ([]*api.LogChunk, error) {
	// Peek at next bytes to detect EOF
	if _, err := r.reader.Peek(1); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}

		return nil, err
	}

	var ts int64
	if err := binary.Read(r.reader, binary.BigEndian, &ts); err != nil {
		return nil, fmt.Errorf("read timestamp: %w", err)
	}

	var count uint32
	if err := binary.Read(r.reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}

	if count > maxSpoolBatchCount {
		return nil, fmt.Errorf("spool batch count %d exceeds max %d", count, maxSpoolBatchCount)
	}

	chunks := make([]*api.LogChunk, 0, count)
	var payloadCRC uint32
	crcWriter := crc32.NewIEEE()

	// We need to CRC the timestamp + count + all chunk data
	_ = binary.Write(crcWriter, binary.BigEndian, ts)
	_ = binary.Write(crcWriter, binary.BigEndian, count)

	for i := uint32(0); i < count; i++ {
		var length uint32
		if err := binary.Read(r.reader, binary.BigEndian, &length); err != nil {
			return nil, fmt.Errorf("read chunk length: %w", err)
		}

		_ = binary.Write(crcWriter, binary.BigEndian, length)
		data := make([]byte, length)
		if _, err := io.ReadFull(r.reader, data); err != nil {
			return nil, fmt.Errorf("read chunk data: %w", err)
		}

		crcWriter.Write(data)
		var chunk api.LogChunk

		if err := proto.Unmarshal(data, &chunk); err != nil {
			return nil, fmt.Errorf("unmarshal chunk: %w", err)
		}

		chunks = append(chunks, &chunk)
	}

	if err := binary.Read(r.reader, binary.BigEndian, &payloadCRC); err != nil {
		return nil, fmt.Errorf("read crc: %w", err)
	}

	if payloadCRC != crcWriter.Sum32() {
		return nil, fmt.Errorf("crc mismatch")
	}

	return chunks, nil
}

// Close closes the underlying reader.
func (r *SpoolReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}

	return nil
}

func encodeBatch(chunks []*api.LogChunk) ([]byte, error) {
	var buf []byte

	// Magic
	buf = append(buf, []byte(spoolMagic)...)

	// Version
	versionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBytes, spoolVersion)
	buf = append(buf, versionBytes...)

	// Timestamp
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(time.Now().UnixNano()))
	buf = append(buf, tsBytes...)

	// Chunk count
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(chunks)))
	buf = append(buf, countBytes...)

	// CRC covers timestamp, count, and all chunks
	crcWriter := crc32.NewIEEE()
	crcWriter.Write(tsBytes)
	crcWriter.Write(countBytes)

	for _, chunk := range chunks {
		data, err := proto.Marshal(chunk)
		if err != nil {
			return nil, fmt.Errorf("marshal chunk: %w", err)
		}

		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))
		buf = append(buf, lengthBytes...)
		crcWriter.Write(lengthBytes)

		buf = append(buf, data...)
		crcWriter.Write(data)
	}

	// CRC
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, crcWriter.Sum32())
	buf = append(buf, crcBytes...)

	return buf, nil
}

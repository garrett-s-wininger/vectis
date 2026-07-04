package logrecord

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestAppendReadRoundTrip(t *testing.T) {
	var buf []byte
	var err error
	buf, err = Append(buf, []byte("hello"))
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	buf, err = Append(buf, []byte{0, 1, 2, 0xff})
	if err != nil {
		t.Fatalf("append second: %v", err)
	}

	first, n, err := Read(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if string(first) != "hello" {
		t.Fatalf("first payload = %q, want hello", first)
	}
	if n != len("hello")+2*LengthSize {
		t.Fatalf("first record bytes = %d, want %d", n, len("hello")+2*LengthSize)
	}

	reader := bytes.NewReader(buf[n:])
	second, n, err := Read(reader)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if !bytes.Equal(second, []byte{0, 1, 2, 0xff}) {
		t.Fatalf("second payload = %v", second)
	}
	if n != 4+2*LengthSize {
		t.Fatalf("second record bytes = %d, want %d", n, 4+2*LengthSize)
	}
}

func TestReadEmptyInputReturnsEOF(t *testing.T) {
	_, _, err := Read(strings.NewReader(""))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read empty err = %v, want EOF", err)
	}
}

func TestReadTruncatedRecordReturnsUnexpectedEOF(t *testing.T) {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, 5)
	buf = append(buf, "hel"...)

	_, _, err := Read(bytes.NewReader(buf))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read truncated err = %v, want unexpected EOF", err)
	}
}

func TestReadRejectsSuffixMismatch(t *testing.T) {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, 3)
	buf = append(buf, "hey"...)
	buf = binary.LittleEndian.AppendUint32(buf, 4)

	_, _, err := Read(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected suffix mismatch error")
	}
	if !strings.Contains(err.Error(), "suffix") {
		t.Fatalf("error = %v, want suffix mismatch", err)
	}
}

func TestReadWithMaxRejectsOversizedPayload(t *testing.T) {
	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, 1024)

	_, _, err := ReadWithMax(bytes.NewReader(buf), 16)
	if err == nil {
		t.Fatal("expected oversized payload error")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("error = %v, want max payload error", err)
	}
}

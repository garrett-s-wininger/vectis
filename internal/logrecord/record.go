package logrecord

import (
	"encoding/binary"
	"fmt"
	"io"
)

const LengthSize = 4

// FramedSize returns the encoded size of one length-prefixed and length-suffixed
// record carrying payloadLen bytes.
func FramedSize(payloadLen int) (int, error) {
	if payloadLen < 0 {
		return 0, fmt.Errorf("record payload length %d is negative", payloadLen)
	}

	if uint64(payloadLen) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("record payload length %d exceeds binary record limit", payloadLen)
	}

	maxInt := int(^uint(0) >> 1)
	if payloadLen > maxInt-2*LengthSize {
		return 0, fmt.Errorf("record payload length %d exceeds addressable buffer size", payloadLen)
	}

	return payloadLen + 2*LengthSize, nil
}

// Append appends one record to dst and returns the extended buffer.
func Append(dst []byte, payload []byte) ([]byte, error) {
	if _, err := FramedSize(len(payload)); err != nil {
		return nil, err
	}

	bodyLen := uint32(len(payload))
	dst = binary.LittleEndian.AppendUint32(dst, bodyLen)
	dst = append(dst, payload...)
	dst = binary.LittleEndian.AppendUint32(dst, bodyLen)
	return dst, nil
}

// Read reads one record from r. The returned byte count includes the prefix,
// payload, and footer bytes consumed from r.
func Read(r io.Reader) ([]byte, int, error) {
	return ReadWithMax(r, 0)
}

// ReadWithMax reads one record from r and rejects payloads larger than
// maxPayloadLen when maxPayloadLen is positive.
func ReadWithMax(r io.Reader, maxPayloadLen int) ([]byte, int, error) {
	if maxPayloadLen < 0 {
		return nil, 0, fmt.Errorf("record max payload length %d is negative", maxPayloadLen)
	}

	var lengthBuf [LengthSize]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, 0, err
	}

	bodyLen := binary.LittleEndian.Uint32(lengthBuf[:])
	if uint64(bodyLen) > uint64(int(^uint(0)>>1)) {
		return nil, 0, fmt.Errorf("record payload length %d exceeds addressable buffer size", bodyLen)
	}
	if maxPayloadLen > 0 && uint64(bodyLen) > uint64(maxPayloadLen) {
		return nil, 0, fmt.Errorf("record payload length %d exceeds max %d", bodyLen, maxPayloadLen)
	}

	payload := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}

	var suffix [LengthSize]byte
	if _, err := io.ReadFull(r, suffix[:]); err != nil {
		return nil, 0, err
	}

	if got := binary.LittleEndian.Uint32(suffix[:]); got != bodyLen {
		return nil, 0, fmt.Errorf("record length suffix %d does not match prefix %d", got, bodyLen)
	}

	return payload, int(bodyLen) + 2*LengthSize, nil
}

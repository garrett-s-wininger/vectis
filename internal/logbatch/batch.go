package logbatch

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	api "vectis/api/gen/go"
)

const (
	recordLengthSize = 4
	recordHeaderSize = 40

	recordFlagTimestamp = 1

	maxPooledBatchBufferSize = 4 << 20
)

var bufferPool = sync.Pool{
	New: func() any {
		return &Buffer{}
	},
}

type Buffer struct {
	buf []byte
}

type Record struct {
	RunID        string
	Data         []byte
	Sequence     int64
	Stream       api.Stream
	Completed    api.RunOutcome
	Timestamp    time.Time
	HasTimestamp bool
}

func MarshalChunks(chunks []*api.LogChunk) ([]byte, error) {
	total, err := recordsSize(chunks)
	if err != nil {
		return nil, err
	}

	return appendChunks(make([]byte, 0, total), chunks)
}

func BorrowMarshalBuffer(chunks []*api.LogChunk) (*Buffer, []byte, error) {
	total, err := recordsSize(chunks)
	if err != nil {
		return nil, nil, err
	}

	buffer := bufferPool.Get().(*Buffer)
	if cap(buffer.buf) < total {
		buffer.buf = make([]byte, 0, total)
	} else {
		buffer.buf = buffer.buf[:0]
	}

	records, err := appendChunks(buffer.buf, chunks)
	if err != nil {
		ReleaseMarshalBuffer(buffer)
		return nil, nil, err
	}

	buffer.buf = records
	return buffer, records, nil
}

func ReleaseMarshalBuffer(buffer *Buffer) {
	if buffer == nil {
		return
	}

	if cap(buffer.buf) > maxPooledBatchBufferSize {
		buffer.buf = nil
	} else {
		buffer.buf = buffer.buf[:0]
	}

	bufferPool.Put(buffer)
}

func appendChunks(buf []byte, chunks []*api.LogChunk) ([]byte, error) {
	for _, chunk := range chunks {
		if chunk == nil {
			return nil, fmt.Errorf("log batch contains nil chunk")
		}

		runID := chunk.GetRunId()
		data := chunk.GetData()
		bodyLen := uint32(recordHeaderSize + len(runID) + len(data))
		buf = binary.LittleEndian.AppendUint32(buf, bodyLen)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(chunk.GetSequence()))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(chunk.GetStream()))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(chunk.GetCompleted()))

		var flags uint32
		var seconds int64
		var nanos int32
		if ts := chunk.GetTimestamp(); ts != nil {
			flags |= recordFlagTimestamp
			seconds = ts.GetSeconds()
			nanos = ts.GetNanos()
		}

		buf = binary.LittleEndian.AppendUint64(buf, uint64(seconds))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(nanos))
		buf = binary.LittleEndian.AppendUint32(buf, flags)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(runID)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, runID...)
		buf = append(buf, data...)
	}

	return buf, nil
}

func DecodeRecords(payload []byte, handle func(Record) error) error {
	for len(payload) > 0 {
		if len(payload) < recordLengthSize {
			return fmt.Errorf("log batch record length truncated")
		}

		bodyLen := int(binary.LittleEndian.Uint32(payload[:recordLengthSize]))
		payload = payload[recordLengthSize:]
		if bodyLen < recordHeaderSize {
			return fmt.Errorf("log batch record body length %d is smaller than header length %d", bodyLen, recordHeaderSize)
		}

		if bodyLen > len(payload) {
			return fmt.Errorf("log batch record body length %d exceeds remaining payload %d", bodyLen, len(payload))
		}

		body := payload[:bodyLen]
		payload = payload[bodyLen:]

		runIDLen := int(binary.LittleEndian.Uint32(body[32:36]))
		dataLen := int(binary.LittleEndian.Uint32(body[36:40]))
		if runIDLen < 0 || dataLen < 0 || recordHeaderSize+runIDLen+dataLen != bodyLen {
			return fmt.Errorf("log batch record run/data lengths do not match body length")
		}

		flags := binary.LittleEndian.Uint32(body[28:32])
		seconds := int64(binary.LittleEndian.Uint64(body[16:24]))
		nanos := int32(binary.LittleEndian.Uint32(body[24:28]))
		if nanos < 0 || nanos >= int32(time.Second/time.Nanosecond) {
			return fmt.Errorf("log batch record timestamp nanosecond value %d is invalid", nanos)
		}

		runStart := recordHeaderSize
		dataStart := runStart + runIDLen
		dataEnd := dataStart + dataLen
		record := Record{
			RunID:     string(body[runStart:dataStart]),
			Data:      append([]byte(nil), body[dataStart:dataEnd]...),
			Sequence:  int64(binary.LittleEndian.Uint64(body[0:8])),
			Stream:    api.Stream(int32(binary.LittleEndian.Uint32(body[8:12]))),
			Completed: api.RunOutcome(int32(binary.LittleEndian.Uint32(body[12:16]))),
		}

		if flags&recordFlagTimestamp != 0 {
			record.Timestamp = time.Unix(seconds, int64(nanos)).UTC()
			record.HasTimestamp = true
		}

		if err := handle(record); err != nil {
			return err
		}
	}

	return nil
}

func recordsSize(chunks []*api.LogChunk) (int, error) {
	var total int
	maxInt := uint64(int(^uint(0) >> 1))
	for _, chunk := range chunks {
		if chunk == nil {
			return 0, fmt.Errorf("log batch contains nil chunk")
		}

		bodyLen := uint64(recordHeaderSize) + uint64(len(chunk.GetRunId())) + uint64(len(chunk.GetData()))
		if bodyLen > uint64(^uint32(0)) {
			return 0, fmt.Errorf("log batch record length %d exceeds binary record limit", bodyLen)
		}

		recordLen := bodyLen + recordLengthSize
		if uint64(total) > maxInt-recordLen {
			return 0, fmt.Errorf("log batch exceeds addressable buffer size")
		}

		total += int(recordLen)
	}

	return total, nil
}

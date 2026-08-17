package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

// IndexEntry maps a message offset to its physical byte position in the log file.
type IndexEntry struct {
	Offset   uint64
	Position uint64
	Length   uint32
}

const IndexEntrySize = 8 + 8 + 4 // 20 bytes

// Segment represents a log data file and its corresponding sparse binary index.
type Segment struct {
	mu         sync.RWMutex
	baseOffset uint64
	nextOffset uint64
	dir        string
	logFile    *os.File
	indexFile  *os.File
	bytesWritten uint64
	maxBytes     uint64
	closed       bool
}

// NewSegment opens or creates a new segment pair (.log and .idx).
func NewSegment(dir string, baseOffset uint64, maxBytes uint64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.idx", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	logInfo, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	s := &Segment{
		baseOffset:   baseOffset,
		nextOffset:   baseOffset,
		dir:          dir,
		logFile:      logFile,
		indexFile:    idxFile,
		bytesWritten: uint64(logInfo.Size()),
		maxBytes:     maxBytes,
	}

	// Recover nextOffset from index file
	idxInfo, err := idxFile.Stat()
	if err == nil && idxInfo.Size() >= IndexEntrySize {
		numEntries := idxInfo.Size() / IndexEntrySize
		lastEntryBytes := make([]byte, IndexEntrySize)
		_, err := idxFile.ReadAt(lastEntryBytes, (numEntries-1)*IndexEntrySize)
		if err == nil {
			lastOffset := binary.BigEndian.Uint64(lastEntryBytes[0:8])
			s.nextOffset = lastOffset + 1
		}
	}

	return s, nil
}

// Append writes a message to the segment log and records its index entry.
func (s *Segment) Append(msg *proto.Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("segment is closed")
	}

	offset := s.nextOffset
	msg.Offset = offset

	// Binary encode message
	// Layout: [8B Offset][8B Timestamp][4B CRC][4B KeyLen][Key][4B ValLen][Val][2B HeaderCount][(K,V)...]
	keyLen := uint32(len(msg.Key))
	valLen := uint32(len(msg.Value))
	headerCount := uint16(len(msg.Headers))

	buf := make([]byte, 0, 8+8+4+4+keyLen+4+valLen+2)
	b8 := make([]byte, 8)
	b4 := make([]byte, 4)
	b2 := make([]byte, 2)

	binary.BigEndian.PutUint64(b8, offset)
	buf = append(buf, b8...)
	binary.BigEndian.PutUint64(b8, uint64(msg.Timestamp))
	buf = append(buf, b8...)
	binary.BigEndian.PutUint32(b4, msg.CRC)
	buf = append(buf, b4...)

	binary.BigEndian.PutUint32(b4, keyLen)
	buf = append(buf, b4...)
	buf = append(buf, msg.Key...)

	binary.BigEndian.PutUint32(b4, valLen)
	buf = append(buf, b4...)
	buf = append(buf, msg.Value...)

	binary.BigEndian.PutUint16(b2, headerCount)
	buf = append(buf, b2...)
	for k, v := range msg.Headers {
		binary.BigEndian.PutUint16(b2, uint16(len(k)))
		buf = append(buf, b2...)
		buf = append(buf, []byte(k)...)
		binary.BigEndian.PutUint16(b2, uint16(len(v)))
		buf = append(buf, b2...)
		buf = append(buf, []byte(v)...)
	}

	msgLength := uint32(len(buf))
	pos := s.bytesWritten

	// Write data
	n, err := s.logFile.Write(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to write to log: %w", err)
	}
	s.bytesWritten += uint64(n)

	// Write index entry: [8B Offset][8B Position][4B Length]
	idxBuf := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(idxBuf[0:8], offset)
	binary.BigEndian.PutUint64(idxBuf[8:16], pos)
	binary.BigEndian.PutUint32(idxBuf[16:20], msgLength)

	if _, err := s.indexFile.Write(idxBuf); err != nil {
		return 0, fmt.Errorf("failed to write to index: %w", err)
	}

	s.nextOffset++
	return offset, nil
}

// ReadAt reads a single message at a specific offset using the index.
func (s *Segment) ReadAt(offset uint64) (*proto.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of bounds [%d, %d)", offset, s.baseOffset, s.nextOffset)
	}

	// Binary search or direct offset calculation in fixed-size index
	indexPos := int64((offset - s.baseOffset) * IndexEntrySize)
	idxBuf := make([]byte, IndexEntrySize)
	if _, err := s.indexFile.ReadAt(idxBuf, indexPos); err != nil {
		return nil, fmt.Errorf("failed to read index at %d: %w", indexPos, err)
	}

	storedOffset := binary.BigEndian.Uint64(idxBuf[0:8])
	pos := binary.BigEndian.Uint64(idxBuf[8:16])
	length := binary.BigEndian.Uint32(idxBuf[16:20])

	if storedOffset != offset {
		return nil, fmt.Errorf("corrupt index: expected offset %d, got %d", offset, storedOffset)
	}

	data := make([]byte, length)
	if _, err := s.logFile.ReadAt(data, int64(pos)); err != nil {
		return nil, fmt.Errorf("failed to read log record: %w", err)
	}

	return decodeMessage(data)
}

func decodeMessage(data []byte) (*proto.Message, error) {
	if len(data) < 26 {
		return nil, io.ErrUnexpectedEOF
	}

	offset := binary.BigEndian.Uint64(data[0:8])
	ts := int64(binary.BigEndian.Uint64(data[8:16]))
	crc := binary.BigEndian.Uint32(data[16:20])

	cursor := 20
	keyLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4
	key := data[cursor : cursor+keyLen]
	cursor += keyLen

	valLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4
	val := data[cursor : cursor+valLen]
	cursor += valLen

	headerCount := int(binary.BigEndian.Uint16(data[cursor : cursor+2]))
	cursor += 2

	headers := make(map[string]string, headerCount)
	for i := 0; i < headerCount; i++ {
		kLen := int(binary.BigEndian.Uint16(data[cursor : cursor+2]))
		cursor += 2
		k := string(data[cursor : cursor+kLen])
		cursor += kLen

		vLen := int(binary.BigEndian.Uint16(data[cursor : cursor+2]))
		cursor += 2
		v := string(data[cursor : cursor+vLen])
		cursor += vLen

		headers[k] = v
	}

	msg := &proto.Message{
		Offset:    offset,
		Timestamp: ts,
		Key:       key,
		Value:     val,
		Headers:   headers,
		CRC:       crc,
	}

	return msg, nil
}

// IsFull returns whether the segment exceeds the max bytes limit.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytesWritten >= s.maxBytes
}

// Sync flushes pending writes to disk.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.indexFile.Sync()
}

// Close closes both file descriptors.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	_ = s.logFile.Close()
	return s.indexFile.Close()
}

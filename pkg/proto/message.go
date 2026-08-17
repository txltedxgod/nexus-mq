package proto

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// Message represents an atomic event/record in NexusMQ.
type Message struct {
	Offset    uint64            `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	CRC       uint32            `json:"crc"`
}

// NewMessage creates a new record with current timestamp and calculates its CRC32.
func NewMessage(key, value []byte, headers map[string]string) *Message {
	msg := &Message{
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Headers:   headers,
	}
	msg.CRC = msg.ComputeCRC()
	return msg
}

// ComputeCRC calculates CRC32 IEEE checksum over the message payload.
func (m *Message) ComputeCRC() uint32 {
	h := crc32.NewIEEE()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(m.Timestamp))
	h.Write(b[:])
	h.Write(m.Key)
	h.Write(m.Value)
	for k, v := range m.Headers {
		h.Write([]byte(k))
		h.Write([]byte(v))
	}
	return h.Sum32()
}

// Validate verifies data integrity against the stored CRC.
func (m *Message) Validate() bool {
	return m.CRC == m.ComputeCRC()
}

// ProduceRequest represents a payload submitted by producers.
type ProduceRequest struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// ProduceResponse returns the assigned offset and partition.
type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Status    string `json:"status"`
}

// ConsumeRequest specifies partition and offset from which to read.
type ConsumeRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Limit     int    `json:"limit"`
}

// ConsumeResponse returns a batch of messages.
type ConsumeResponse struct {
	Topic     string     `json:"topic"`
	Partition int        `json:"partition"`
	Messages  []*Message `json:"messages"`
	NextOffset uint64    `json:"next_offset"`
}

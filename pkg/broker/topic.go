package broker

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/txltedxgod/nexus-mq/pkg/log"
	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

// Partition represents a single commit log partition.
type Partition struct {
	ID        int
	TopicName string
	Log       *log.Log
}

// Topic coordinates multiple partitions and partitioning strategies.
type Topic struct {
	mu         sync.RWMutex
	name       string
	dataDir    string
	partitions []*Partition
	roundRobin uint64
}

// NewTopic creates a new topic with the specified number of partitions.
func NewTopic(name string, numPartitions int, dataDir string, maxSegmentBytes uint64) (*Topic, error) {
	if numPartitions <= 0 {
		numPartitions = 1
	}

	t := &Topic{
		name:       name,
		dataDir:    dataDir,
		partitions: make([]*Partition, numPartitions),
	}

	topicDir := filepath.Join(dataDir, name)
	for i := 0; i < numPartitions; i++ {
		partDir := filepath.Join(topicDir, fmt.Sprintf("partition-%d", i))
		l, err := log.NewLog(log.Config{
			Directory:       partDir,
			MaxSegmentBytes: maxSegmentBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to init partition %d: %w", i, err)
		}
		t.partitions[i] = &Partition{
			ID:        i,
			TopicName: name,
			Log:       l,
		}
	}

	return t, nil
}

// Name returns topic name.
func (t *Topic) Name() string {
	return t.name
}

// NumPartitions returns count of partitions.
func (t *Topic) NumPartitions() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.partitions)
}

// GetPartition returns partition by ID.
func (t *Topic) GetPartition(id int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id < 0 || id >= len(t.partitions) {
		return nil, fmt.Errorf("invalid partition %d (topic %s has %d partitions)", id, t.name, len(t.partitions))
	}
	return t.partitions[id], nil
}

// SelectPartition chooses a partition based on message key or round-robin.
func (t *Topic) SelectPartition(key []byte) int {
	t.mu.RLock()
	numParts := len(t.partitions)
	t.mu.RUnlock()

	if len(key) > 0 {
		h := fnv.New32a()
		h.Write(key)
		return int(h.Sum32()) % numParts
	}

	idx := atomic.AddUint64(&t.roundRobin, 1)
	return int(idx) % numParts
}

// Publish writes a message to the chosen or specified partition.
func (t *Topic) Publish(req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	partID := req.Partition
	if partID < 0 {
		partID = t.SelectPartition([]byte(req.Key))
	}

	p, err := t.GetPartition(partID)
	if err != nil {
		return nil, err
	}

	msg := proto.NewMessage([]byte(req.Key), []byte(req.Value), req.Headers)
	offset, err := p.Log.Append(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to append to partition %d: %w", partID, err)
	}

	return &proto.ProduceResponse{
		Topic:     t.name,
		Partition: partID,
		Offset:    offset,
		Timestamp: msg.Timestamp,
		Status:    "ACK",
	}, nil
}

// Close closes all partition logs.
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.partitions {
		_ = p.Log.Close()
	}
	return nil
}

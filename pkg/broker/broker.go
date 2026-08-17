package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/txltedxgod/nexus-mq/pkg/metrics"
	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

// BrokerConfig holds configuration for the NexusMQ broker engine.
type BrokerConfig struct {
	NodeID          string
	DataDir         string
	MaxSegmentBytes uint64
	DefaultParts    int
}

// Broker coordinates topics, partition storage, and metrics.
type Broker struct {
	mu      sync.RWMutex
	nodeID  string
	dataDir string
	cfg     BrokerConfig
	topics  map[string]*Topic
	metrics *metrics.Metrics
	closed  bool
}

// NewBroker initializes a new broker instance.
func NewBroker(cfg BrokerConfig) (*Broker, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = 10 * 1024 * 1024
	}
	if cfg.DefaultParts <= 0 {
		cfg.DefaultParts = 4
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create broker data dir: %w", err)
	}

	b := &Broker{
		nodeID:  cfg.NodeID,
		dataDir: cfg.DataDir,
		cfg:     cfg,
		topics:  make(map[string]*Topic),
		metrics: metrics.NewMetrics(),
	}

	// Auto-discover existing topics from disk
	if err := b.discoverTopics(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Broker) discoverTopics() error {
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			topicName := entry.Name()
			// Count partitions
			partEntries, err := os.ReadDir(filepath.Join(b.dataDir, topicName))
			if err != nil {
				continue
			}
			partsCount := 0
			for _, pe := range partEntries {
				if pe.IsDir() {
					partsCount++
				}
			}
			if partsCount == 0 {
				partsCount = 1
			}

			topic, err := NewTopic(topicName, partsCount, b.dataDir, b.cfg.MaxSegmentBytes)
			if err == nil {
				b.topics[topicName] = topic
			}
		}
	}
	return nil
}

// CreateTopic creates a new topic with the given partition count.
func (b *Broker) CreateTopic(name string, partitions int) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("broker is shutting down")
	}

	if _, exists := b.topics[name]; exists {
		return b.topics[name], nil
	}

	if partitions <= 0 {
		partitions = b.cfg.DefaultParts
	}

	topic, err := NewTopic(name, partitions, b.dataDir, b.cfg.MaxSegmentBytes)
	if err != nil {
		return nil, err
	}

	b.topics[name] = topic
	return topic, nil
}

// GetTopic returns topic by name, or nil if not found.
func (b *Broker) GetTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// ListTopics returns all topic names and metadata.
func (b *Broker) ListTopics() []map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res := make([]map[string]interface{}, 0, len(b.topics))
	for name, topic := range b.topics {
		partsInfo := make([]map[string]interface{}, 0)
		for i := 0; i < topic.NumPartitions(); i++ {
			p, err := topic.GetPartition(i)
			if err == nil {
				partsInfo = append(partsInfo, map[string]interface{}{
					"partition_id":  i,
					"oldest_offset": p.Log.OldestOffset(),
					"latest_offset": p.Log.LatestOffset(),
					"segments":      p.Log.TotalSegments(),
				})
			}
		}
		res = append(res, map[string]interface{}{
			"topic":      name,
			"partitions": partsInfo,
		})
	}
	return res
}

// Publish routes and writes a message.
func (b *Broker) Publish(req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	b.mu.RLock()
	topic, exists := b.topics[req.Topic]
	b.mu.RUnlock()

	if !exists {
		var err error
		topic, err = b.CreateTopic(req.Topic, b.cfg.DefaultParts)
		if err != nil {
			return nil, fmt.Errorf("failed to auto-create topic %s: %w", req.Topic, err)
		}
	}

	resp, err := topic.Publish(req)
	if err != nil {
		return nil, err
	}

	b.metrics.IncProduced(uint64(len(req.Value)))
	return resp, nil
}

// Consume fetches a batch of messages from a topic partition.
func (b *Broker) Consume(req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	topic := b.GetTopic(req.Topic)
	if topic == nil {
		return nil, fmt.Errorf("topic %s not found", req.Topic)
	}

	part, err := topic.GetPartition(req.Partition)
	if err != nil {
		return nil, err
	}

	msgs, nextOffset, err := part.Log.ReadBatch(req.Offset, req.Limit)
	if err != nil {
		return nil, err
	}

	totalBytes := 0
	for _, m := range msgs {
		totalBytes += len(m.Value)
	}
	b.metrics.IncConsumed(uint64(len(msgs)), uint64(totalBytes))

	return &proto.ConsumeResponse{
		Topic:      req.Topic,
		Partition:  req.Partition,
		Messages:   msgs,
		NextOffset: nextOffset,
	}, nil
}

// Metrics returns the broker's performance metrics registry.
func (b *Broker) Metrics() *metrics.Metrics {
	return b.metrics
}

// Close gracefully closes the broker.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for _, topic := range b.topics {
		_ = topic.Close()
	}
	return nil
}

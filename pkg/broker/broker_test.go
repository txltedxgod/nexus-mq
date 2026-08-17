package broker

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

func TestBrokerPublishAndConsume(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexus_broker_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	b, err := NewBroker(BrokerConfig{
		NodeID:          "node-1",
		DataDir:         tempDir,
		MaxSegmentBytes: 4096,
		DefaultParts:    3,
	})
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	topicName := "orders.v1"
	_, err = b.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Publish messages across partitions
	numMsgs := 60
	for i := 0; i < numMsgs; i++ {
		req := &proto.ProduceRequest{
			Topic:     topicName,
			Partition: i % 3,
			Key:       fmt.Sprintf("user-%d", i),
			Value:     fmt.Sprintf(`{"order_id": %d, "amount": %0.2f}`, i, float64(i)*10.5),
			Headers:   map[string]string{"env": "production"},
		}
		resp, err := b.Publish(req)
		if err != nil {
			t.Fatalf("Publish failed at %d: %v", i, err)
		}
		if resp.Status != "ACK" {
			t.Fatalf("Expected ACK, got %s", resp.Status)
		}
	}

	// Consume from partition 0
	consumeResp, err := b.Consume(&proto.ConsumeRequest{
		Topic:     topicName,
		Partition: 0,
		Offset:    0,
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(consumeResp.Messages) != 20 {
		t.Fatalf("Expected 20 messages in partition 0, got %d", len(consumeResp.Messages))
	}
}

func TestConcurrentPublishing(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexus_concurrent_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	b, err := NewBroker(BrokerConfig{
		NodeID:       "node-concurrent",
		DataDir:      tempDir,
		DefaultParts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	topic := "telemetry"
	concurrency := 10
	messagesPerWorker := 50
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for m := 0; m < messagesPerWorker; m++ {
				req := &proto.ProduceRequest{
					Topic:     topic,
					Partition: -1, // round-robin auto
					Key:       fmt.Sprintf("device-%d", workerID),
					Value:     fmt.Sprintf("ping-%d-%d", workerID, m),
				}
				_, err := b.Publish(req)
				if err != nil {
					t.Errorf("Worker %d failed to publish msg %d: %v", workerID, m, err)
				}
			}
		}(w)
	}

	wg.Wait()
	if b.Metrics().GetTotalProduced() != uint64(concurrency*messagesPerWorker) {
		t.Fatalf("Expected total produced %d, got %d", concurrency*messagesPerWorker, b.Metrics().GetTotalProduced())
	}
}

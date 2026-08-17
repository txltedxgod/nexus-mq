package log

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

func TestLogAppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexus_log_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := Config{
		Directory:       tempDir,
		MaxSegmentBytes: 1024, // Small segment to force rollover
	}

	l, err := NewLog(cfg)
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}
	defer l.Close()

	totalMessages := 50
	for i := 0; i < totalMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("payload-data-value-number-%d", i))
		headers := map[string]string{"type": "telemetry", "sender": "test"}

		msg := proto.NewMessage(key, val, headers)
		offset, err := l.Append(msg)
		if err != nil {
			t.Fatalf("Append failed at index %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("Expected offset %d, got %d", i, offset)
		}
	}

	// Read individual messages
	for i := 0; i < totalMessages; i++ {
		msg, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read failed for offset %d: %v", i, err)
		}
		if string(msg.Key) != fmt.Sprintf("key-%d", i) {
			t.Fatalf("Key mismatch: expected key-%d, got %s", i, string(msg.Key))
		}
		if !msg.Validate() {
			t.Fatalf("CRC validation failed for offset %d", i)
		}
	}

	// Test batch read
	msgs, nextOffset, err := l.ReadBatch(10, 20)
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(msgs) != 20 {
		t.Fatalf("Expected 20 messages in batch, got %d", len(msgs))
	}
	if nextOffset != 30 {
		t.Fatalf("Expected nextOffset 30, got %d", nextOffset)
	}

	// Verify segment rollover happened
	if l.TotalSegments() <= 1 {
		t.Fatalf("Expected multiple segments after writing %d messages, got %d", totalMessages, l.TotalSegments())
	}
}

func TestLogRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "nexus_log_recovery_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := Config{
		Directory:       filepath.Join(tempDir, "partition-0"),
		MaxSegmentBytes: 512,
	}

	// 1. First session
	l1, err := NewLog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		msg := proto.NewMessage([]byte("k"), []byte("v"), nil)
		_, err := l1.Append(msg)
		if err != nil {
			t.Fatal(err)
		}
	}
	l1.Sync()
	l1.Close()

	// 2. Second session (Recovery)
	l2, err := NewLog(cfg)
	if err != nil {
		t.Fatalf("Failed to recover log: %v", err)
	}
	defer l2.Close()

	if l2.LatestOffset() != 20 {
		t.Fatalf("Expected recovered next offset 20, got %d", l2.LatestOffset())
	}

	// Append more
	msg := proto.NewMessage([]byte("k20"), []byte("v20"), nil)
	offset, err := l2.Append(msg)
	if err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}
	if offset != 20 {
		t.Fatalf("Expected offset 20, got %d", offset)
	}
}

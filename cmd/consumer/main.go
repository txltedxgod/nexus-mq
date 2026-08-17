package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "NexusMQ broker HTTP URL")
	topic := flag.String("topic", "benchmark.events", "Topic to consume")
	partition := flag.Int("partition", 0, "Partition ID")
	startOffset := flag.Uint64("offset", 0, "Initial offset")
	flag.Parse()

	log.Printf("[CONSUMER] Tail-consuming topic '%s' [partition %d] from offset %d...", *topic, *partition, *startOffset)

	offset := *startOffset
	client := &http.Client{Timeout: 5 * time.Second}

	for {
		url := fmt.Sprintf("%s/api/v1/consume?topic=%s&partition=%d&offset=%d&limit=50", *serverURL, *topic, *partition, offset)
		resp, err := client.Get(url)
		if err != nil {
			log.Printf("[WARN] Error fetching messages: %v. Retrying...", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var consumeResp proto.ConsumeResponse
		if err := json.NewDecoder(resp.Body).Decode(&consumeResp); err != nil {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp.Body.Close()

		if len(consumeResp.Messages) == 0 {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		for _, msg := range consumeResp.Messages {
			fmt.Printf("[#%d] Key=%s | Value=%s | CRC32=0x%x\n", msg.Offset, string(msg.Key), string(msg.Value), msg.CRC)
			offset = msg.Offset + 1
		}
	}
}

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/txltedxgod/nexus-mq/pkg/proto"
)

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "NexusMQ broker HTTP URL")
	topic := flag.String("topic", "benchmark.events", "Target topic")
	totalMsgs := flag.Int("messages", 10000, "Total number of messages to produce")
	concurrency := flag.Int("concurrency", 8, "Number of concurrent worker goroutines")
	flag.Parse()

	log.Printf("[PRODUCER] Starting benchmark: %d messages to topic '%s' using %d workers", *totalMsgs, *topic, *concurrency)

	start := time.Now()
	msgsPerWorker := *totalMsgs / *concurrency
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 5 * time.Second}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < msgsPerWorker; i++ {
				req := proto.ProduceRequest{
					Topic:     *topic,
					Partition: -1,
					Key:       fmt.Sprintf("user-%d-%d", workerID, i),
					Value:     fmt.Sprintf(`{"id": %d, "worker": %d, "timestamp": %d}`, i, workerID, time.Now().UnixNano()),
					Headers:   map[string]string{"source": "benchmark-cli"},
				}
				body, _ := json.Marshal(req)
				resp, err := client.Post(fmt.Sprintf("%s/api/v1/produce", *serverURL), "application/json", bytes.NewReader(body))
				if err != nil {
					log.Printf("Worker %d publish error: %v", workerID, err)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	throughput := float64(*totalMsgs) / elapsed.Seconds()

	fmt.Printf("\n--- Benchmark Results ---\n")
	fmt.Printf("Total Messages: %d\n", *totalMsgs)
	fmt.Printf("Time Elapsed:   %s\n", elapsed)
	fmt.Printf("Throughput:     %.2f msgs/sec\n", throughput)
}

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/txltedxgod/nexus-mq/pkg/broker"
	"github.com/txltedxgod/nexus-mq/pkg/server"
)

const banner = `
  _   _                      __  __  ____  
 | \ | | _____  ___   _ ___ |  \/  |/ __ \ 
 |  \| |/ _ \ \/ / | | / __|| |\/| | |  | |
 | |\  |  __/>  <| |_| \__ \| |  | | |__| |
 |_| \_|\___/_/\_\\__,_|___/|_|  |_|\___\_\
 high-throughput distributed commit-log broker
`

func main() {
	nodeID := flag.String("node-id", "nexus-node-01", "Unique broker node identifier")
	httpAddr := flag.String("http-addr", ":8080", "HTTP and Dashboard listen address")
	dataDir := flag.String("data-dir", "./nexus-data", "Storage directory for commit logs")
	maxSegmentMB := flag.Uint64("segment-size-mb", 10, "Max log segment size before rollover in MB")
	defaultParts := flag.Int("default-partitions", 4, "Default partitions for auto-created topics")
	flag.Parse()

	fmt.Println(banner)
	log.Printf("[INFO] Initializing NexusMQ node '%s' on %s...", *nodeID, *httpAddr)

	b, err := broker.NewBroker(broker.BrokerConfig{
		NodeID:          *nodeID,
		DataDir:         *dataDir,
		MaxSegmentBytes: *maxSegmentMB * 1024 * 1024,
		DefaultParts:    *defaultParts,
	})
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize broker: %v", err)
	}

	httpSrv := server.NewHTTPServer(*httpAddr, b)
	go func() {
		log.Printf("[INFO] HTTP API & Web Dashboard listening at http://localhost%s", *httpAddr)
		if err := httpSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[INFO] Received shutdown signal, closing broker...")
	_ = httpSrv.Close()
	_ = b.Close()
	log.Println("[INFO] NexusMQ broker stopped cleanly.")
}

package main

import (
	"fmt"
	"time"
)

type EventMessage struct {
	Topic     string
	Payload   string
	Timestamp time.Time
}

func main() {
	msg := EventMessage{
		Topic:     "orders.events",
		Payload:   `{"order_id": "ord_9941", "amount": 149.99, "currency": "USD"}`,
		Timestamp: time.Now().UTC(),
	}
	fmt.Printf("[Nexus-MQ Demo] Published message to '%s' at %s\nPayload: %s\n", msg.Topic, msg.Timestamp, msg.Payload)
}

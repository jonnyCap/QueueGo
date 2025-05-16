package main

import (
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
)

func main() {
	// Connect to the broker
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	log.Println("[backend] Connected to broker")

	// Example hardcoded JWT (in a real app you'd load/generate this)
	jwt := []byte("my-backend-jwt")

	// Subscribe to a topic (e.g., topicID 1)
	sub := blink.NewSubscribeFrame(jwt, 1)
	if err := blink.SendFrame(conn, sub); err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}
	log.Println("[backend] Subscribed to topic 1")

	// Listen for messages
	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("failed to read frame: %v", err)
		}

		switch f := frame.(type) {
		case *blink.MessageFrame:
			log.Printf("[backend] Received message: %s", string(f.Payload))
			// Process the message (e.g., update DB, trigger job, etc.)
			process(f.TopicID, f.Payload)

		default:
			log.Printf("[backend] Ignored frame type: %T", f)
		}
	}
}

func process(topicID uint32, payload []byte) {
	log.Printf("[backend] Processing message from topic %d: %s", topicID, payload)
	// TODO: Replace this with actual business logic
}

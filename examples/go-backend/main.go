package main

import (
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

func main() {
	// Connect to the broker
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	log.Println("[backend] Connected to broker")

	topicName := "orders"
	topicID := blink.HashTopic(topicName)
	queueKey := "secret-orders-key"
	masterKey := "my-master-key" // Matches config.yaml

	// Generate a valid JWT with subscribe permission
	jwtToken, err := auth.GenerateToken(masterKey, "backend-service", queueKey, "subscribe")
	if err != nil {
		log.Fatalf("failed to generate JWT: %v", err)
	}

	// Subscribe to the topic
	sub := blink.NewSubscribeFrame([]byte(jwtToken), topicID)
	if err := blink.SendFrame(conn, sub); err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}
	log.Printf("[backend] Subscribed to topic %q (ID: %d)", topicName, topicID)

	// Listen for incoming messages and key updates
	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("failed to read frame: %v", err)
		}

		switch f := frame.(type) {
		case *blink.MessageFrame:
			log.Printf("[backend] Received message: %s", string(f.Payload))
			process(f.TopicID, f.Payload)

		case *blink.KeyUpdateFrame:
			log.Printf("[backend] Received KeyUpdate for topic %d, new key: %s", f.TopicID, string(f.NewKey))

		default:
			log.Printf("[backend] Ignored frame type: %T", f)
		}
	}
}

func process(topicID uint32, payload []byte) {
	log.Printf("[backend] Processing message from topic %d: %s", topicID, payload)
}


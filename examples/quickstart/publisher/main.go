package main

import (
	"log"
	"net"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	topicName := "quickstart"
	topicID := blink.HashTopic(topicName)
	queueKey := "my-secret-key"
	masterKey := "my-master-key"

	// Generate tokens
	createToken, _ := auth.GenerateToken(masterKey, "admin", queueKey, "create")
	pubToken, _ := auth.GenerateToken(masterKey, "publisher", queueKey, "publish")

	// 1. Create topic
	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), topicName, 0x00)); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// 2. Publish message
	payload := []byte("Hello, QueueGo from Quickstart Publisher!")
	if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(pubToken), topicID, payload)); err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}

	log.Printf("Published message to %q (ID: %d): %s", topicName, topicID, payload)
	time.Sleep(100 * time.Millisecond)
}

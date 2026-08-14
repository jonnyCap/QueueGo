package main

import (
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	topicName := "orders"
	topicID := blink.HashTopic(topicName)
	queueKey := "secret-orders-key"
	masterKey := "my-master-key" // Matches config.yaml

	createToken, err := auth.GenerateToken(masterKey, "admin", queueKey, "create")
	if err != nil {
		log.Fatalf("failed to generate create token: %v", err)
	}

	subToken, err := auth.GenerateToken(masterKey, "client-subscriber", queueKey, "subscribe")
	if err != nil {
		log.Fatalf("failed to generate subscribe token: %v", err)
	}

	pubToken, err := auth.GenerateToken(masterKey, "client-publisher", queueKey, "publish")
	if err != nil {
		log.Fatalf("failed to generate publish token: %v", err)
	}

	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), topicName, 0x00)); err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}
	log.Printf("Created topic %q (ID: %d)", topicName, topicID)

	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}
	log.Printf("Subscribed to topic %q", topicName)

	if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(pubToken), topicID, []byte("Hello from Go!"))); err != nil {
		log.Fatalf("failed to publish: %v", err)
	}
	log.Println("Published test message")

	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("read frame error: %v", err)
		}
		if msg, ok := frame.(*blink.MessageFrame); ok {
			log.Println("Got:", string(msg.Payload))
		}
	}
}


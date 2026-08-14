package main

import (
	"log"
	"net"

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

	subToken, _ := auth.GenerateToken(masterKey, "subscriber", queueKey, "subscribe")

	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("Subscribed to topic %q (ID: %d). Listening for messages...", topicName, topicID)

	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("Connection closed: %v", err)
		}
		if msg, ok := frame.(*blink.MessageFrame); ok {
			log.Printf("Received message on topic %d: %s", msg.TopicID, string(msg.Payload))
		}
	}
}

package main

import (
	"flag"
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "QueueGo broker address")
	masterKey := flag.String("master-key", "my-master-key", "Broker master key")
	currentQueueKey := flag.String("current-key", "orders-secret-key-1", "Current queue access key")
	newQueueKey := flag.String("new-key", "orders-secret-key-2", "New queue access key to rotate to")
	topicName := flag.String("topic", "orders", "Topic name")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*topicName)

	rotateToken, err := auth.GenerateToken(*masterKey, "security-admin", *currentQueueKey, "rotate")
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	log.Printf("[KeyRotator] Requesting key rotation for %q (ID: %d)", *topicName, topicID)
	log.Printf("  Current key: %s", *currentQueueKey)
	log.Printf("  New key:     %s", *newQueueKey)

	frame := blink.NewRotateKeyFrame([]byte(rotateToken), topicID, []byte(*newQueueKey))
	if err := blink.SendFrame(conn, frame); err != nil {
		log.Fatalf("Failed to send ROTATE_KEY frame: %v", err)
	}

	log.Println("[KeyRotator] Key rotation request sent successfully! All active subscribers have been notified via KEY_UPDATE.")
}

package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Item      string    `json:"item"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "QueueGo broker address")
	masterKey := flag.String("master-key", "my-master-key", "Broker master key")
	queueKey := flag.String("queue-key", "orders-secret-key-1", "Orders queue key")
	topicName := flag.String("topic", "orders", "Topic name")
	workerID := flag.String("worker", "worker-1", "Worker identifier")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*topicName)

	subToken, err := auth.GenerateToken(*masterKey, *workerID, *queueKey, "subscribe")
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("[%s] Subscribed to topic %q (ID: %d). Waiting for orders...", *workerID, *topicName, topicID)

	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("[%s] Connection lost: %v", *workerID, err)
		}

		switch f := frame.(type) {
		case *blink.MessageFrame:
			var order Order
			if err := json.Unmarshal(f.Payload, &order); err != nil {
				log.Printf("[%s] Raw message: %s", *workerID, string(f.Payload))
				continue
			}

			log.Printf("[%s] Processing order %s for %s ($%.2f for %s)...",
				*workerID, order.ID, order.Customer, order.Amount, order.Item)
			// Simulate processing time
			time.Sleep(200 * time.Millisecond)
			log.Printf("[%s] Finished order %s successfully.", *workerID, order.ID)

		case *blink.KeyUpdateFrame:
			log.Printf("[%s] NOTICE: Received KeyUpdateFrame for topic %d! New key: %q",
				*workerID, f.TopicID, string(f.NewKey))

		default:
			log.Printf("[%s] Received frame: %T", *workerID, f)
		}
	}
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
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
	count := flag.Int("count", 10, "Number of orders to produce (0 for infinite)")
	delay := flag.Duration("delay", 1*time.Second, "Delay between orders")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*topicName)

	// Create topic & generate publish token
	createToken, _ := auth.GenerateToken(*masterKey, "storefront-admin", *queueKey, "create")
	pubToken, _ := auth.GenerateToken(*masterKey, "storefront-service", *queueKey, "publish")

	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), *topicName, 0x00)); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("[Storefront] Connected to QueueGo. Producing orders to %q (ID: %d)...", *topicName, topicID)

	items := []string{"Laptop Pro 16", "Wireless Mouse", "Mechanical Keyboard", "4K Monitor", "USB-C Hub"}
	customers := []string{"Alice Smith", "Bob Jones", "Charlie Brown", "Diana Prince"}

	i := 1
	for {
		if *count > 0 && i > *count {
			break
		}

		order := Order{
			ID:        fmt.Sprintf("ORD-%04d", i),
			Customer:  customers[rand.Intn(len(customers))],
			Item:      items[rand.Intn(len(items))],
			Amount:    float64(rand.Intn(1000)+50) + 0.99,
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(order)
		if err != nil {
			log.Fatalf("Failed to serialize order: %v", err)
		}

		if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(pubToken), topicID, data)); err != nil {
			log.Printf("[Storefront] Publish error: %v", err)
		} else {
			log.Printf("[Storefront] Published order %s (%s - $%.2f)", order.ID, order.Item, order.Amount)
		}

		i++
		time.Sleep(*delay)
	}

	log.Println("[Storefront] Completed producing orders.")
}

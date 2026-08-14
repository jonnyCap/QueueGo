package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

type ChatMessage struct {
	User      string `json:"user"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp"`
}

func main() {
	username := flag.String("user", "Anonymous", "Username for chat")
	room := flag.String("room", "general", "Chat room name")
	addr := flag.String("addr", "127.0.0.1:9000", "QueueGo broker address")
	masterKey := flag.String("master-key", "my-master-key", "Broker master key")
	queueKey := flag.String("queue-key", "chat-room-secret", "Chat room access key")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker at %s: %v", *addr, err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*room)

	// 1. Generate auth tokens
	createToken, err := auth.GenerateToken(*masterKey, *username, *queueKey, "create")
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}
	subPubToken, err := auth.GenerateToken(*masterKey, *username, *queueKey, "subscribe", "publish")
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	// 2. Create room (idempotent on broker)
	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), *room, 0x00)); err != nil {
		log.Fatalf("Failed to create chat room: %v", err)
	}

	// 3. Subscribe to the room
	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subPubToken), topicID)); err != nil {
		log.Fatalf("Failed to subscribe to room: %v", err)
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf(" Welcome to QueueGo Chat Room: #%s\n", *room)
	fmt.Printf(" Logged in as: %s\n", *username)
	fmt.Printf(" Type a message and press Enter to send.\n")
	fmt.Printf(" Press Ctrl+C to leave.\n")
	fmt.Printf("========================================\n\n")

	// Broadcast join message
	joinMsg, _ := json.Marshal(ChatMessage{
		User:      "SYSTEM",
		Text:      fmt.Sprintf("--> %s joined the room.", *username),
		Timestamp: time.Now().Format("15:04:05"),
	})
	_ = blink.SendFrame(conn, blink.NewPublishFrame([]byte(subPubToken), topicID, joinMsg))

	// Listen for incoming messages
	go func() {
		for {
			frame, err := blink.ReadFrame(conn)
			if err != nil {
				log.Println("\nDisconnected from chat server.")
				os.Exit(0)
			}
			if msg, ok := frame.(*blink.MessageFrame); ok {
				var chat ChatMessage
				if err := json.Unmarshal(msg.Payload, &chat); err == nil {
					if chat.User == "SYSTEM" {
						fmt.Printf("\r[%s] %s\n> ", chat.Timestamp, chat.Text)
					} else if chat.User != *username {
						fmt.Printf("\r[%s] <%s>: %s\n> ", chat.Timestamp, chat.User, chat.Text)
					}
				}
			}
		}
	}()

	// Handle graceful shutdown on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		leaveMsg, _ := json.Marshal(ChatMessage{
			User:      "SYSTEM",
			Text:      fmt.Sprintf("<-- %s left the room.", *username),
			Timestamp: time.Now().Format("15:04:05"),
		})
		_ = blink.SendFrame(conn, blink.NewPublishFrame([]byte(subPubToken), topicID, leaveMsg))
		_ = blink.SendFrame(conn, blink.NewUnsubscribeFrame([]byte(subPubToken), topicID))
		conn.Close()
		fmt.Println("\nGoodbye!")
		os.Exit(0)
	}()

	// Read user input and publish
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text != "" {
			chat := ChatMessage{
				User:      *username,
				Text:      text,
				Timestamp: time.Now().Format("15:04:05"),
			}
			data, _ := json.Marshal(chat)
			if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(subPubToken), topicID, data)); err != nil {
				log.Printf("Failed to send message: %v", err)
				break
			}
		}
		fmt.Print("> ")
	}
}

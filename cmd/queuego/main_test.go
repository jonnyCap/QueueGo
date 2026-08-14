package main

import (
	"net"
	"testing"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
	"github.com/jonnycap/queuego/internal/broker"
	tcp "github.com/jonnycap/queuego/internal/transport"
)

func TestEndToEndBlinkServer(t *testing.T) {
	masterKey := "integration-test-master-key"
	auth.SetMasterKey(masterKey)

	b := broker.NewBroker(nil)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}
	defer listener.Close()

	go func() {
		_ = tcp.Serve(listener, b)
	}()

	addr := listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to test server: %v", err)
	}
	defer conn.Close()

	queueKey := "secret-queue-key"
	topicName := "demo"
	topicID := blink.HashTopic(topicName)

	createToken, err := auth.GenerateToken(masterKey, "admin", queueKey, "create")
	if err != nil {
		t.Fatal(err)
	}
	subToken, err := auth.GenerateToken(masterKey, "client", queueKey, "subscribe")
	if err != nil {
		t.Fatal(err)
	}
	pubToken, err := auth.GenerateToken(masterKey, "client", queueKey, "publish")
	if err != nil {
		t.Fatal(err)
	}

	// 1. Send CREATE
	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), topicName, 0x00)); err != nil {
		t.Fatalf("failed to send CREATE: %v", err)
	}

	// Small pause for server to register topic
	time.Sleep(50 * time.Millisecond)

	// 2. Send SUBSCRIBE
	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		t.Fatalf("failed to send SUBSCRIBE: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// 3. Send PUBLISH
	testPayload := []byte("Hello Blink over TCP!")
	if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(pubToken), topicID, testPayload)); err != nil {
		t.Fatalf("failed to send PUBLISH: %v", err)
	}

	// 4. Read MESSAGE
	done := make(chan struct{})
	go func() {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			t.Errorf("failed to read frame: %v", err)
			return
		}
		msg, ok := frame.(*blink.MessageFrame)
		if !ok {
			t.Errorf("expected *MessageFrame, got %T", frame)
			return
		}
		if msg.TopicID != topicID {
			t.Errorf("expected topic ID %d, got %d", topicID, msg.TopicID)
		}
		if string(msg.Payload) != string(testPayload) {
			t.Errorf("expected payload %q, got %q", testPayload, msg.Payload)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MESSAGE frame")
	}
}

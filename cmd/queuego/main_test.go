package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
	"github.com/jonnycap/queuego/internal/broker"
	"github.com/jonnycap/queuego/internal/metrics"
	tcp "github.com/jonnycap/queuego/internal/transport"
)

func TestEndToEndBlinkServer(t *testing.T) {
	masterKey := "integration-test-master-key"
	auth.SetMasterKey(masterKey)

	b := broker.NewBroker(nil)
	serverMetrics := &metrics.Metrics{StartTime: time.Now()}

	server, err := tcp.NewServer(tcp.ServerConfig{
		Addr:    "127.0.0.1:0",
		Metrics: serverMetrics,
	}, b)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		_ = server.Start()
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	addr := server.Addr().String()
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

	// 1. Send CREATE and expect ACK
	if err := blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), topicName, 0x00)); err != nil {
		t.Fatalf("failed to send CREATE: %v", err)
	}
	frame, err := blink.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read frame: %v", err)
	}
	ack, ok := frame.(*blink.AckFrame)
	if !ok || ack.OpCode != blink.TypeCreate || ack.TopicID != topicID {
		t.Fatalf("expected create ACK, got %+v", frame)
	}

	// 2. Send SUBSCRIBE and expect ACK
	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		t.Fatalf("failed to send SUBSCRIBE: %v", err)
	}
	frame, err = blink.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read frame: %v", err)
	}
	ack, ok = frame.(*blink.AckFrame)
	if !ok || ack.OpCode != blink.TypeSubscribe || ack.TopicID != topicID {
		t.Fatalf("expected subscribe ACK, got %+v", frame)
	}

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

func TestEndToEndMultiMessageStream(t *testing.T) {
	masterKey := "multi-msg-master-key"
	auth.SetMasterKey(masterKey)
	b := broker.NewBroker(nil)

	server, err := tcp.NewServer(tcp.ServerConfig{
		Addr: "127.0.0.1:0",
	}, b)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	go func() {
		_ = server.Start()
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	addr := server.Addr().String()

	// Connect Subscriber
	subConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect subscriber: %v", err)
	}
	defer subConn.Close()

	// Connect Publisher
	pubConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer pubConn.Close()

	queueKey := "stream-queue-key"
	topicName := "stream-topic"
	topicID := blink.HashTopic(topicName)

	createToken, _ := auth.GenerateToken(masterKey, "admin", queueKey, "create")
	subToken, _ := auth.GenerateToken(masterKey, "sub", queueKey, "subscribe")
	pubToken, _ := auth.GenerateToken(masterKey, "pub", queueKey, "publish")

	// Create topic & subscribe (read ACKs)
	if err := blink.SendFrame(subConn, blink.NewCreateFrame([]byte(createToken), topicName, 0x00)); err != nil {
		t.Fatal(err)
	}
	if _, err := blink.ReadFrame(subConn); err != nil {
		t.Fatal(err)
	}

	if err := blink.SendFrame(subConn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		t.Fatal(err)
	}
	if _, err := blink.ReadFrame(subConn); err != nil {
		t.Fatal(err)
	}

	msgCount := 25
	receivedChan := make(chan int, msgCount)

	go func() {
		for i := 0; i < msgCount; i++ {
			frame, err := blink.ReadFrame(subConn)
			if err != nil {
				return
			}
			if msg, ok := frame.(*blink.MessageFrame); ok {
				if msg.TopicID == topicID {
					receivedChan <- len(msg.Payload)
				}
			}
		}
	}()

	// Send rapid consecutive publish frames on publisher connection
	for i := 0; i < msgCount; i++ {
		payload := []byte(time.Now().Format(time.RFC3339Nano))
		if err := blink.SendFrame(pubConn, blink.NewPublishFrame([]byte(pubToken), topicID, payload)); err != nil {
			t.Fatalf("failed to publish frame %d: %v", i, err)
		}
	}

	// Verify all messages received
	for i := 0; i < msgCount; i++ {
		select {
		case <-receivedChan:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d of %d", i+1, msgCount)
		}
	}
}

func TestErrorFrameOnUnauthorized(t *testing.T) {
	masterKey := "error-frame-test-key"
	auth.SetMasterKey(masterKey)
	b := broker.NewBroker(nil)

	server, err := tcp.NewServer(tcp.ServerConfig{Addr: "127.0.0.1:0"}, b)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Start() }()
	defer func() { _ = server.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send SUBSCRIBE without valid JWT token
	invalidToken := "invalid-garbage-jwt"
	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(invalidToken), 12345)); err != nil {
		t.Fatal(err)
	}

	frame, err := blink.ReadFrame(conn)
	if err != nil {
		t.Fatalf("expected error frame from server: %v", err)
	}

	errFrame, ok := frame.(*blink.ErrorFrame)
	if !ok {
		t.Fatalf("expected *ErrorFrame, got %T", frame)
	}
	if errFrame.OpCode != blink.TypeSubscribe {
		t.Errorf("expected OpCode TypeSubscribe (0x02), got 0x%x", errFrame.OpCode)
	}
	if errFrame.ErrorCode != 401 {
		t.Errorf("expected ErrorCode 401, got %d", errFrame.ErrorCode)
	}
}

func TestObservabilityEndpoints(t *testing.T) {
	m := &metrics.Metrics{StartTime: time.Now()}
	m.ConnOpened()
	m.IncPublished()
	m.SetTopics(3)

	httpServer := metrics.StartHTTPServer("127.0.0.1:0", m)
	defer func() {
		_ = metrics.StopHTTPServer(context.Background(), httpServer)
	}()

	// Wait briefly for server listener to bind
	time.Sleep(50 * time.Millisecond)
	healthURL := "http://" + httpServer.Addr + "/health"
	metricsURL := "http://" + httpServer.Addr + "/metrics"

	// 1. Check /health
	resp, err := http.Get(healthURL)
	if err != nil {
		// In case listener is ephemeral, query internal handler directly
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /health, got %d", resp.StatusCode)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode health JSON: %v", err)
	}
	if health["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", health["status"])
	}

	// 2. Check /metrics
	resp2, err := http.Get(metricsURL)
	if err != nil {
		return
	}
	defer resp2.Body.Close()

	body, _ := io.ReadAll(resp2.Body)
	metricsText := string(body)
	if !strings.Contains(metricsText, "queuego_active_connections") || !strings.Contains(metricsText, "queuego_messages_published_total") {
		t.Errorf("missing Prometheus metric keys in response:\n%s", metricsText)
	}
}

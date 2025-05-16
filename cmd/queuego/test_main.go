package main

import (
	"net"
	"testing"
	"time"

	blink "github.com/jonnycap/blink/go"
)

func TestBlinkQueue(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	jwt := []byte("test-jwt")

	if err := blink.SendFrame(conn, blink.NewCreateFrame(jwt, "topic", 0x00)); err != nil {
		t.Fatal(err)
	}
	if err := blink.SendFrame(conn, blink.NewSubscribeFrame(jwt, 1)); err != nil {
		t.Fatal(err)
	}
	if err := blink.SendFrame(conn, blink.NewPublishFrame(jwt, 1, []byte("from test"))); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})

	go func() {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			t.Fatal(err)
		}
		if msg, ok := frame.(*blink.MessageFrame); ok {
			if string(msg.Payload) != "from test" {
				t.Errorf("unexpected payload: %s", msg.Payload)
			}
			close(done)
		} else {
			t.Errorf("unexpected frame type: %T", frame)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

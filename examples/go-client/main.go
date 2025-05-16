package main

import (
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	jwt := []byte("test-jwt")

	blink.SendFrame(conn, blink.NewCreateFrame(jwt, "demo", 0x00))
	blink.SendFrame(conn, blink.NewSubscribeFrame(jwt, 1))
	blink.SendFrame(conn, blink.NewPublishFrame(jwt, 1, []byte("Hello from Go!")))

	for {
		frame, _ := blink.ReadFrame(conn)
		if msg, ok := frame.(*blink.MessageFrame); ok {
			log.Println("Got:", string(msg.Payload))
		}
	}
}

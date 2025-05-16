package tcp

import (
	"log"
	"net"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/broker"
)

func StartServer(addr string, b *broker.Broker) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Printf("listening on %s", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, b)
	}
}

func handleConn(conn net.Conn, b *broker.Broker) {
	defer conn.Close()
	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Printf("read frame error: %v", err)
			return
		}
		switch f := frame.(type) {
		case *blink.CreateFrame:
			topicID, err := b.CreateTopic(f)
			if err != nil {
				log.Println("create error:", err)
			} else {
				log.Println("created topic", topicID)
			}
		case *blink.SubscribeFrame:
			b.Subscribe(f, conn)
		case *blink.UnsubscribeFrame:
			b.Unsubscribe(f, conn)
		case *blink.PublishFrame:
			b.Publish(f)
		case *blink.RotateKeyFrame:
			b.RotateKey(f)
		default:
			log.Printf("unsupported frame type: %T", f)
		}
	}
}

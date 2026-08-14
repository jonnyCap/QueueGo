package tcp

import (
	"bufio"
	"log"
	"net"
	"sync"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/broker"
)

type safeConn struct {
	net.Conn
	mu sync.Mutex
}

func (s *safeConn) Write(b []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Conn.Write(b)
}

func StartServer(addr string, b *broker.Broker) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Printf("listening on %s", addr)

	return Serve(l, b)
}

func Serve(l net.Listener, b *broker.Broker) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go handleConn(conn, b)
	}
}

func handleConn(conn net.Conn, b *broker.Broker) {
	sConn := &safeConn{Conn: conn}
	defer func() {
		b.RemoveSubscriber(sConn)
		sConn.Close()
	}()

	reader := bufio.NewReader(sConn)
	for {
		frame, err := blink.ParseFrame(reader)
		if err != nil {
			return
		}
		switch f := frame.(type) {
		case *blink.CreateFrame:
			if topicID, err := b.CreateTopic(f); err != nil {
				log.Println("create error:", err)
			} else {
				log.Printf("created topic %q (ID: %d)", f.TopicName, topicID)
			}
		case *blink.SubscribeFrame:
			if err := b.Subscribe(f, sConn); err != nil {
				log.Println("subscribe error:", err)
			}
		case *blink.UnsubscribeFrame:
			if err := b.Unsubscribe(f, sConn); err != nil {
				log.Println("unsubscribe error:", err)
			}
		case *blink.PublishFrame:
			if err := b.Publish(f); err != nil {
				log.Println("publish error:", err)
			}
		case *blink.RotateKeyFrame:
			if err := b.RotateKey(f); err != nil {
				log.Println("rotate key error:", err)
			}
		default:
			log.Printf("unsupported frame type: %T", f)
		}
	}
}


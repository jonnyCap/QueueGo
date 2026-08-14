package tcp

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/broker"
	"github.com/jonnycap/queuego/internal/metrics"
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

// ServerConfig configures the TCP/TLS transport server.
type ServerConfig struct {
	Addr      string
	TLSConfig *tls.Config
	Metrics   *metrics.Metrics
}

// Server manages the network listener, active connections, and graceful shutdown.
type Server struct {
	listener   net.Listener
	broker     *broker.Broker
	metrics    *metrics.Metrics
	conns      sync.Map // net.Conn -> struct{}
	mu         sync.Mutex
	tlsEnabled bool
	closed     bool
	done       chan struct{}
}

// NewServer creates a new Server instance.
func NewServer(cfg ServerConfig, b *broker.Broker) (*Server, error) {
	var l net.Listener
	var err error
	tlsEnabled := false

	if cfg.TLSConfig != nil {
		l, err = tls.Listen("tcp", cfg.Addr, cfg.TLSConfig)
		tlsEnabled = true
	} else {
		l, err = net.Listen("tcp", cfg.Addr)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.Addr, err)
	}

	m := cfg.Metrics
	if m == nil {
		m = metrics.DefaultMetrics
	}

	return &Server{
		listener:   l,
		broker:     b,
		metrics:    m,
		tlsEnabled: tlsEnabled,
		done:       make(chan struct{}),
	}, nil
}

// Addr returns the listener's network address.
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Start runs the server accept loop.
func (s *Server) Start() error {
	defer close(s.done)
	log.Printf("QueueGo server listening on %s (TLS: %v)", s.listener.Addr(), s.IsTLS())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return nil
			}
			return err
		}

		s.conns.Store(conn, struct{}{})
		s.metrics.ConnOpened()
		go s.handleConn(conn)
	}
}

// IsTLS returns true if the server is running with TLS encryption.
func (s *Server) IsTLS() bool {
	return s.tlsEnabled
}

// Shutdown gracefully stops the listener and closes all active client connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// 1. Close listener so no new connections are accepted
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	// 2. Close all active client connections
	s.conns.Range(func(key, _ interface{}) bool {
		if c, ok := key.(net.Conn); ok {
			_ = c.Close()
		}
		return true
	})

	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	return err
}

func (s *Server) handleConn(conn net.Conn) {
	sConn := &safeConn{Conn: conn}
	defer func() {
		s.broker.RemoveSubscriber(sConn)
		s.conns.Delete(conn)
		s.metrics.ConnClosed()
		_ = sConn.Close()
	}()

	reader := bufio.NewReader(sConn)
	for {
		frame, err := blink.ParseFrame(reader)
		if err != nil {
			return
		}

		switch f := frame.(type) {
		case *blink.CreateFrame:
			if topicID, err := s.broker.CreateTopic(f); err != nil {
				log.Println("create error:", err)
				_ = blink.SendFrame(sConn, blink.NewErrorFrame(blink.TypeCreate, 400, err.Error()))
			} else {
				log.Printf("created topic %q (ID: %d)", f.TopicName, topicID)
				_ = blink.SendFrame(sConn, blink.NewAckFrame(blink.TypeCreate, topicID))
			}

		case *blink.SubscribeFrame:
			if err := s.broker.Subscribe(f, sConn); err != nil {
				log.Println("subscribe error:", err)
				_ = blink.SendFrame(sConn, blink.NewErrorFrame(blink.TypeSubscribe, 401, err.Error()))
			} else {
				_ = blink.SendFrame(sConn, blink.NewAckFrame(blink.TypeSubscribe, f.TopicID))
			}

		case *blink.UnsubscribeFrame:
			if err := s.broker.Unsubscribe(f, sConn); err != nil {
				log.Println("unsubscribe error:", err)
				_ = blink.SendFrame(sConn, blink.NewErrorFrame(blink.TypeUnsubscribe, 400, err.Error()))
			} else {
				_ = blink.SendFrame(sConn, blink.NewAckFrame(blink.TypeUnsubscribe, f.TopicID))
			}

		case *blink.PublishFrame:
			if err := s.broker.Publish(f); err != nil {
				log.Println("publish error:", err)
				_ = blink.SendFrame(sConn, blink.NewErrorFrame(blink.TypePublish, 401, err.Error()))
			} else {
				s.metrics.IncPublished()
			}

		case *blink.RotateKeyFrame:
			if err := s.broker.RotateKey(f); err != nil {
				log.Println("rotate key error:", err)
				_ = blink.SendFrame(sConn, blink.NewErrorFrame(blink.TypeRotateKey, 403, err.Error()))
			} else {
				_ = blink.SendFrame(sConn, blink.NewAckFrame(blink.TypeRotateKey, f.TopicID))
			}

		default:
			log.Printf("unsupported frame type: %T", f)
			_ = blink.SendFrame(sConn, blink.NewErrorFrame(0x00, 400, "unsupported frame type"))
		}
	}
}

// StartServer starts a standalone server on the given address (backwards compatible).
func StartServer(addr string, b *broker.Broker) error {
	srv, err := NewServer(ServerConfig{Addr: addr}, b)
	if err != nil {
		return err
	}
	return srv.Start()
}

// Serve serves on an existing net.Listener (backwards compatible).
func Serve(l net.Listener, b *broker.Broker) error {
	srv := &Server{
		listener: l,
		broker:   b,
		metrics:  metrics.DefaultMetrics,
		done:     make(chan struct{}),
	}
	return srv.Start()
}

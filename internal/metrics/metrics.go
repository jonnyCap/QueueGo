package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// Metrics holds atomic counters for broker observability.
type Metrics struct {
	ActiveConnections int64
	TopicsTotal       int64
	MessagesPublished uint64
	MessagesDelivered uint64
	MessagesDropped   uint64
	StartTime         time.Time
}

var DefaultMetrics = &Metrics{
	StartTime: time.Now(),
}

func (m *Metrics) ConnOpened() {
	atomic.AddInt64(&m.ActiveConnections, 1)
}

func (m *Metrics) ConnClosed() {
	atomic.AddInt64(&m.ActiveConnections, -1)
}

func (m *Metrics) IncPublished() {
	atomic.AddUint64(&m.MessagesPublished, 1)
}

func (m *Metrics) IncDelivered() {
	atomic.AddUint64(&m.MessagesDelivered, 1)
}

func (m *Metrics) IncDropped() {
	atomic.AddUint64(&m.MessagesDropped, 1)
}

func (m *Metrics) SetTopics(n int64) {
	atomic.StoreInt64(&m.TopicsTotal, n)
}

// StartHTTPServer starts the observability server on the given address.
func StartHTTPServer(addr string, m *Metrics) *http.Server {
	if m == nil {
		m = DefaultMetrics
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Since(m.StartTime).Seconds()
		resp := map[string]interface{}{
			"status":             "healthy",
			"uptime_seconds":     int64(uptime),
			"active_connections": atomic.LoadInt64(&m.ActiveConnections),
			"topics_total":       atomic.LoadInt64(&m.TopicsTotal),
			"messages_published": atomic.LoadUint64(&m.MessagesPublished),
			"messages_delivered": atomic.LoadUint64(&m.MessagesDelivered),
			"messages_dropped":   atomic.LoadUint64(&m.MessagesDropped),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		fmt.Fprintf(w, "# HELP queuego_active_connections Current number of active TCP client connections\n")
		fmt.Fprintf(w, "# TYPE queuego_active_connections gauge\n")
		fmt.Fprintf(w, "queuego_active_connections %d\n\n", atomic.LoadInt64(&m.ActiveConnections))

		fmt.Fprintf(w, "# HELP queuego_topics_total Total number of active topics\n")
		fmt.Fprintf(w, "# TYPE queuego_topics_total gauge\n")
		fmt.Fprintf(w, "queuego_topics_total %d\n\n", atomic.LoadInt64(&m.TopicsTotal))

		fmt.Fprintf(w, "# HELP queuego_messages_published_total Total messages published through broker\n")
		fmt.Fprintf(w, "# TYPE queuego_messages_published_total counter\n")
		fmt.Fprintf(w, "queuego_messages_published_total %d\n\n", atomic.LoadUint64(&m.MessagesPublished))

		fmt.Fprintf(w, "# HELP queuego_messages_delivered_total Total messages delivered to subscribers\n")
		fmt.Fprintf(w, "# TYPE queuego_messages_delivered_total counter\n")
		fmt.Fprintf(w, "queuego_messages_delivered_total %d\n\n", atomic.LoadUint64(&m.MessagesDelivered))

		fmt.Fprintf(w, "# HELP queuego_messages_dropped_total Total messages dropped due to slow subscribers\n")
		fmt.Fprintf(w, "# TYPE queuego_messages_dropped_total counter\n")
		fmt.Fprintf(w, "queuego_messages_dropped_total %d\n", atomic.LoadUint64(&m.MessagesDropped))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		_ = server.ListenAndServe()
	}()

	return server
}

// StopHTTPServer gracefully shuts down the HTTP server.
func StopHTTPServer(ctx context.Context, s *http.Server) error {
	if s == nil {
		return nil
	}
	return s.Shutdown(ctx)
}

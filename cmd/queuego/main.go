package main

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/jonnycap/queuego/internal/auth"
	"github.com/jonnycap/queuego/internal/broker"
	"github.com/jonnycap/queuego/internal/metrics"
	tcp "github.com/jonnycap/queuego/internal/transport"
)

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Config struct {
	MasterKey  string    `yaml:"master_key"`
	ListenAddr string    `yaml:"listen_addr"`
	DbPath     string    `yaml:"db_path"`
	HttpAddr   string    `yaml:"http_addr"`
	TLS        TLSConfig `yaml:"tls"`
}

func loadConfig() Config {
	configFile := "configs/config.yaml"
	if custom := os.Getenv("CONFIG_FILE"); custom != "" {
		configFile = custom
	}

	f, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("failed to open config (%s): %v", configFile, err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		log.Fatalf("failed to decode config: %v", err)
	}
	return cfg
}

func main() {
	cfg := loadConfig()
	auth.SetMasterKey(cfg.MasterKey)

	// 1. Initialize persistent store
	store, err := broker.OpenStore(cfg.DbPath)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}

	// 2. Initialize broker
	b := broker.NewBroker(store)

	// 3. Initialize metrics & HTTP server
	brokerMetrics := metrics.DefaultMetrics
	var httpServer *http.Server
	if cfg.HttpAddr != "" {
		httpServer = metrics.StartHTTPServer(cfg.HttpAddr, brokerMetrics)
		log.Printf("QueueGo metrics & health HTTP server listening on %s", cfg.HttpAddr)
	}

	// 4. Configure TLS if enabled
	var tlsConf *tls.Config
	if cfg.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			log.Fatalf("failed to load TLS certificate/key: %v", err)
		}
		tlsConf = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	// 5. Initialize TCP server
	server, err := tcp.NewServer(tcp.ServerConfig{
		Addr:      cfg.ListenAddr,
		TLSConfig: tlsConf,
		Metrics:   brokerMetrics,
	}, b)
	if err != nil {
		log.Fatalf("failed to create TCP server: %v", err)
	}

	// 6. Launch TCP accept loop in background
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Start()
	}()

	// 7. Listen for OS termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErr:
		if err != nil {
			log.Printf("server encountered error: %v", err)
		}
	case sig := <-sigChan:
		log.Printf("received signal %v, initiating graceful shutdown...", sig)
	}

	// 8. Graceful shutdown sequence with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("draining active TCP connections...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("error stopping TCP server: %v", err)
	}

	if httpServer != nil {
		log.Println("stopping HTTP metrics server...")
		if err := metrics.StopHTTPServer(shutdownCtx, httpServer); err != nil {
			log.Printf("error stopping HTTP server: %v", err)
		}
	}

	log.Println("closing persistent database store...")
	if err := store.Close(); err != nil {
		log.Printf("error closing BadgerDB: %v", err)
	}

	log.Println("QueueGo shutdown completed cleanly.")
}

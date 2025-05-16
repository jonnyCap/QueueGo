package main

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/jonnycap/queuego/internal/auth"
	"github.com/jonnycap/queuego/internal/broker"
	tcp "github.com/jonnycap/queuego/internal/transport"
)

type Config struct {
	MasterKey  	string `yaml:"master_key"`
	ListenAddr 	string `yaml:"listen_addr"`
	DbPath		string `yaml:"db_path"`
}

func loadConfig() Config {
	f, err := os.Open("configs/config.yaml")
	if err != nil {
		log.Fatalf("failed to open config: %v", err)
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

	// Initialize persistent store
	store, err := broker.OpenStore(cfg.DbPath)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	// Pass store to broker
	b := broker.NewBroker(store)

	if err := tcp.StartServer(cfg.ListenAddr, b); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

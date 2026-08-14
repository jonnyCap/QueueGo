# QueueGo 🚀

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?logo=go&logoColor=white)](https://golang.org)
[![Protocol](https://img.shields.io/badge/Protocol-Blink%20TCP-FF6B6B)](../Blink)
[![Storage](https://img.shields.io/badge/Storage-BadgerDB-blueviolet)](https://github.com/dgraph-io/badger)
[![Observability](https://img.shields.io/badge/Metrics-Prometheus-E6522C?logo=prometheus&logoColor=white)](http://localhost:9001/metrics)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

**QueueGo** is a high-performance, lightweight Pub/Sub message broker built in Go. It operates over the binary **Blink TCP Protocol**, combining sub-millisecond in-memory message delivery, embedded durable storage via **BadgerDB**, stateless **JWT-based authentication**, dynamic access key rotation, and native **Prometheus observability**.

---

## 💡 The Inspiration

Modern cloud architectures and edge computing environments require messaging infrastructure that is fast, resilient, and simple to operate. Traditional brokers often introduce substantial operational complexity, heavy runtime dependencies (e.g. JVM), or lack fine-grained cryptographic authorization at the queue level.

QueueGo was created to deliver:
- **Maximum Throughput with Minimal Overhead:** Direct binary TCP framing via Blink, avoiding JSON serialization and HTTP handshake costs.
- **Embedded Durability:** Native persistent queues powered by BadgerDB LSM-tree storage without requiring an external database cluster.
- **Zero-Trust Per-Topic Security:** Cryptographic JWT validation on every packet, binding access permissions (`publish`, `subscribe`, `rotate`) directly to per-queue keys.
- **Operational Simplicity:** Single static binary, low memory footprint, zero external runtime dependencies, built-in TLS, and Prometheus metrics.

---

## ⚙️ Architecture & How It Works

```
                        +---------------------------------------------+
                        |                 QueueGo Broker              |
                        |                                             |
[ Go / Python / JS ]    |   +------------------+                      |
      Clients           |   |   TCP Transport  | (TLS 1.2/1.3, Auth)  |
         |              |   +--------+---------+                      |
    (Blink TCP)         |            |                                |
         +-------------------------->|                                |
                        |            v                                |
                        |   +------------------+                      |
                        |   |  Broker Engine   |<---> [  BadgerDB  ]  |
                        |   | (Topic Dispatch) |      (Persistence)   |
                        |   +--------+---------+                      |
                        |            |                                |
                        |            v                                |
                        |   +------------------+                      |
                        |   | HTTP Metrics API | (:9001/metrics,      |
                        |   |  & Health Check  |  :9001/health)       |
                        |   +------------------+                      |
                        +---------------------------------------------+
```

1. **Transport Layer (`internal/transport`):** Manages concurrent TCP connections, optional TLS encryption, and framed packet parsing.
2. **Broker Engine (`internal/broker`):** High-speed in-memory routing table mapping 32-bit hashed `TopicID`s to active client subscribers with mutex synchronization.
3. **Persistence Store (`internal/broker/store.go`):** Durable BadgerDB log for crash-resilient queue metadata and message history.
4. **Auth & Key Rotation (`internal/auth`):** HMAC-SHA256 JWT verification. Supports runtime `ROTATE_KEY` commands that trigger automatic `KEY_UPDATE` notifications to authorized subscribers.
5. **Observability (`internal/metrics`):** Embedded HTTP server exposing `/metrics` (Prometheus) and `/health`.

---

## ⚡ Quickstart

### 1. Build and Run Locally

```bash
# Clone and enter directory
cd QueueGo

# Run the broker
go run cmd/queuego/main.go
```

### 2. Run with Docker & Docker Compose

```bash
# Start broker container
docker compose up -d
```

### 3. Verify Health & Metrics

```bash
curl http://localhost:9001/health
curl http://localhost:9001/metrics
```

---

## ⚙️ Configuration

Configuration is managed via `configs/config.yaml` or through the `CONFIG_FILE` environment variable:

```yaml
# Master HMAC Secret for signing & validating JWTs
master_key: "my-master-key"

# Primary TCP listener for Blink clients
listen_addr: "127.0.0.1:9000"

# BadgerDB data directory for persistence
db_path: "data/"

# HTTP endpoint for Prometheus metrics & health probes
http_addr: "127.0.0.1:9001"

# TLS encryption in transit (optional)
tls:
  enabled: false
  cert_file: ""
  key_file: ""
```

---

## 📚 Real-World Examples & Patterns

Explore practical, runnable examples in the [`examples/`](./examples/) directory:

- [**Quickstart**](./examples/quickstart/): Basic connect, topic creation, publishing, and subscribing in Go.
- [**Chat Room**](./examples/chat-room/): Multi-user terminal broadcast chat demonstrating topic multiplexing.
- [**Order Processing**](./examples/order-processing/): E-commerce microservice workflow with live key rotation (`ROTATE_KEY`).
- [**IoT Telemetry**](./examples/iot-telemetry/): High-frequency sensor ingestion and real-time dashboarding.
- [**Python Client**](./examples/python-client/): Real-time Python producer and consumer.
- [**Node.js Client**](./examples/js-client/): Event-driven JavaScript publisher and subscriber.

---

## 📂 Repository Structure

```
QueueGo/
├── cmd/
│   └── queuego/            # Application entrypoint (main.go)
├── configs/                # Default YAML configuration
├── internal/
│   ├── auth/               # JWT token creation, claims & validation
│   ├── broker/             # Core broker engine & BadgerDB storage
│   ├── metrics/            # Prometheus collectors & HTTP health server
│   └── transport/          # Non-blocking TCP server & TLS handler
├── examples/               # End-to-end multi-language example apps
├── dockerfile              # Container image definition
└── docker-compose.yaml     # Containerized deployment spec
```

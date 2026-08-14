# Quickstart Example

The simplest minimal demonstration of connecting, subscribing, and publishing messages with **QueueGo** and **Blink**.

---

## How to Run

### 1. Start QueueGo Broker
```bash
go run cmd/queuego/main.go
```

### 2. Start Subscriber
In **Terminal 1**:
```bash
cd examples/quickstart/subscriber
go run main.go
```

### 3. Run Publisher
In **Terminal 2**:
```bash
cd examples/quickstart/publisher
go run main.go
```

# Microservices Order Processing & Dynamic Key Rotation

This example demonstrates an asynchronous event-driven e-commerce workflow with **QueueGo**, showcasing decoupled order distribution and zero-downtime key rotation.

---

## How QueueGo Solves This Problem

1. **Decoupled Architecture**: The Storefront service produces orders without needing to know which or how many worker instances are processing them.
2. **Dynamic Key Rotation Without Downtime**: Security policies often mandate rotating access keys. QueueGo allows the security admin to rotate a topic's key with the `ROTATE_KEY` packet. The broker instantly sends `KEY_UPDATE` packets to all connected subscribers, allowing continuous operation without disconnecting clients.

---

## Architecture

```
[Storefront Producer] ---> PUBLISH(orders) ---> [QueueGo Broker] ---> MESSAGE(orders) ---> [Order Worker]
                                                       ^
                                                       | ROTATE_KEY
                                                       |
                                            [Key Rotator Admin]
```

---

## Running the Example

### 1. Start the QueueGo Broker
```bash
go run cmd/queuego/main.go
```

### 2. Start the Order Worker
In **Terminal 1**:
```bash
cd examples/order-processing/worker
go run main.go -worker worker-alpha
```

### 3. Start the Order Producer
In **Terminal 2**:
```bash
cd examples/order-processing/producer
go run main.go -count 20 -delay 1s
```
You will see orders being published and processed in real time.

### 4. Trigger Key Rotation Live
While the producer and worker are running, in **Terminal 3**:
```bash
cd examples/order-processing/key-rotator
go run main.go -current-key orders-secret-key-1 -new-key orders-secret-key-2
```
Look at **Terminal 1 (Worker)**: you will observe the live `KEY_UPDATE` notification received from QueueGo without any connection interruption.

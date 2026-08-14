# Real-Time Chat Room Example

This example demonstrates how to build an interactive, multi-user terminal chat application powered by **QueueGo** and the **Blink** protocol.

---

## How QueueGo Solves This Problem

In real-time multi-user applications like chat services:
1. **Low Latency & High Concurrency**: Blink's lightweight binary framing delivers messages between users with minimal overhead over persistent TCP connections.
2. **Channel-Based Isolation**: Each chat room is mapped directly to a topic via `blink.HashTopic(roomName)`.
3. **Role & Room Authorization**: Every user authenticates with a signed JWT carrying the topic `queue_key` and `"subscribe"` + `"publish"` permissions.

---

## Running the Example

### 1. Start the QueueGo Broker
From the repository root:
```bash
go run cmd/queuego/main.go
```

### 2. Start Chat Clients
Open two or more terminal windows and run:

**Terminal 1 (Alice):**
```bash
cd examples/chat-room
go run main.go -user Alice -room general
```

**Terminal 2 (Bob):**
```bash
cd examples/chat-room
go run main.go -user Bob -room general
```

Type messages in either terminal to see them broadcast in real-time to all users in `#general`.

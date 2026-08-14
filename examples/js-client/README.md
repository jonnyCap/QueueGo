# Node.js / JavaScript Client Example

This example demonstrates how to connect to the **QueueGo** message broker using the **Blink JavaScript SDK** in Node.js.

---

## What It Demonstrates

1. **Pure Built-in JWT Generation:** Uses Node.js built-in `crypto` module to sign HS256 JWT tokens (zero npm package dependencies required).
2. **Event-Driven Pub/Sub:** Connects to QueueGo over TCP and manages message streaming with the `BlinkClient` EventEmitter.
3. **Dynamic Hashing & Topic Registration:** Automatically computes 32-bit FNV-1a topic IDs matching Go and Python.
4. **Publishing & Real-Time Delivery:** Publishes JSON event payloads and processes incoming messages asynchronously.

---

## How to Run

### 1. Start the QueueGo Broker
From the `QueueGo` root directory:
```bash
go run cmd/queuego/main.go
```

### 2. Run the Node.js Example
```bash
node examples/js-client/index.js
```

---

## Sample Output

```
==================================================
 QueueGo Node.js Client (Blink Protocol) Example
==================================================

[1/4] Connecting to QueueGo broker at 127.0.0.1:9000...
✓ Connected successfully!

[2/4] Registering topic "nodejs-events" on broker...
✓ Topic registered! (Hashed ID: 1571775791)

[3/4] Subscribing to topic "nodejs-events"...
✓ Subscribed and listening for incoming events!

[4/4] Publishing events to "nodejs-events"...
  -> Published event #1

  [Subscriber Received] TopicID: 1571775791 | Event: ORDER_CREATED_1 | Msg: Order #1 created via Node.js SDK
  -> Published event #2

  [Subscriber Received] TopicID: 1571775791 | Event: ORDER_CREATED_2 | Msg: Order #2 created via Node.js SDK
  -> Published event #3

  [Subscriber Received] TopicID: 1571775791 | Event: ORDER_CREATED_3 | Msg: Order #3 created via Node.js SDK

✓ Completed! Total messages received by subscriber: 3
✓ Connection closed gracefully.
```

# Python Client Example

This example demonstrates how to connect to the **QueueGo** message broker using the **Blink Python SDK** (`asyncio`).

---

## What It Demonstrates

1. **Self-contained Token Generation:** Signs HS256 JWTs using Python's built-in `hmac` and `hashlib` modules (zero external dependencies required).
2. **Connecting to QueueGo:** Establishes an asynchronous TCP connection using `BlinkClient`.
3. **Topic Creation & Hashing:** Automatically hashes the topic name to its 32-bit FNV-1a integer ID.
4. **Asynchronous Subscription:** Receives and parses real-time JSON payloads using callbacks.
5. **Publishing Messages:** Streams structured events to the topic.

---

## How to Run

### 1. Start the QueueGo Broker
From the `QueueGo` root directory:
```bash
go run cmd/queuego/main.go
```

### 2. Run the Python Example
```bash
python3 examples/python-client/main.py
```

---

## Sample Output

```
==================================================
 QueueGo Python Client (Blink Protocol) Example
==================================================

[1/4] Connecting to QueueGo broker at 127.0.0.1:9000...
✓ Connected successfully!

[2/4] Registering topic 'python-events' on broker...
✓ Topic registered! (Hashed ID: 3624899142)

[3/4] Subscribing to topic 'python-events'...
✓ Subscribed and listening for incoming messages!

[4/4] Publishing events to 'python-events'...
  -> Published event #1

  [Subscriber Received] TopicID: 3624899142 | Event: PAYMENT_COMPLETED_1 | Msg: Payment #1 processed successfully via Python SDK
  -> Published event #2

  [Subscriber Received] TopicID: 3624899142 | Event: PAYMENT_COMPLETED_2 | Msg: Payment #2 processed successfully via Python SDK
  -> Published event #3

  [Subscriber Received] TopicID: 3624899142 | Event: PAYMENT_COMPLETED_3 | Msg: Payment #3 processed successfully via Python SDK

✓ Completed! Total messages received by subscriber: 3
✓ Connection closed gracefully.
```

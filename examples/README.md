# QueueGo Examples & Inspiration

This directory contains complete, runnable example applications demonstrating various architectural patterns and real-world use cases using **QueueGo** and the **Blink** protocol.

---

## Available Examples

| Example | Description | Concepts Demonstrated |
|---|---|---|
| [**Quickstart**](./quickstart/) | Minimal publisher & subscriber | Basic connect, CREATE, SUBSCRIBE, and PUBLISH packets |
| [**Chat Room**](./chat-room/) | Multi-user broadcast terminal chat | Real-time bi-directional messaging, topic multiplexing |
| [**Order Processing**](./order-processing/) | E-commerce microservices event streaming | Decoupled background workers, live key rotation (`ROTATE_KEY` / `KEY_UPDATE`) |
| [**IoT Telemetry**](./iot-telemetry/) | Edge sensor telemetry streaming & live dashboard | Low-bandwidth binary framing, high-frequency metrics ingestion |
| [**Python Client**](./python-client/) | Real-time Python event producer & consumer | Zero-dependency async/sync Blink Python SDK integration |
| [**Node.js Client**](./js-client/) | Event-driven JavaScript publisher & subscriber | Zero-dependency Node.js Blink SDK integration |

---

## Quick Testing

Before running any example, start the broker:
```bash
go run cmd/queuego/main.go
```

Then navigate into any example directory and follow its `README.md`.

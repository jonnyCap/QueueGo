# IoT Telemetry Ingestion & Real-Time Monitoring

This example demonstrates how **QueueGo** and **Blink** handle high-frequency sensor telemetry streams from multiple edge IoT devices.

---

## How QueueGo Solves This Problem

1. **Ultra-Low Framing Overhead**: IoT edge devices operate under constrained bandwidth and compute power. Blink's compact binary headers (fixed control bytes + uint32 topic IDs) minimize network serialization and packet parsing costs compared to bulky JSON/HTTP polling or complex protocols.
2. **Deterministic Topic Hashing**: Devices hash their topic strings into a 4-byte integer (`blink.HashTopic("telemetry")`), reducing header size on every subsequent packet.
3. **Stateless JWT Authorization**: Sensors authenticate per-request without requiring the broker to maintain heavy session state tables per sensor.

---

## Running the Example

### 1. Start the QueueGo Broker
```bash
go run cmd/queuego/main.go
```

### 2. Start the Telemetry Dashboard
In **Terminal 1**:
```bash
cd examples/iot-telemetry/dashboard
go run main.go
```

### 3. Launch Multiple Sensor Nodes
Open additional terminals and simulate multiple sensor nodes:

**Terminal 2 (Sensor 1):**
```bash
cd examples/iot-telemetry/sensor
go run main.go -sensor node-livingroom -loc "Living Room" -interval 500ms
```

**Terminal 3 (Sensor 2):**
```bash
cd examples/iot-telemetry/sensor
go run main.go -sensor node-serverroom -loc "Server Room Rack A" -interval 1s
```

Observe the live streaming data formatted in the central dashboard in Terminal 1.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

type TelemetryData struct {
	SensorID    string  `json:"sensor_id"`
	Location    string  `json:"location"`
	Temperature float64 `json:"temp_c"`
	Humidity    float64 `json:"humidity_pct"`
	BatteryPct  int     `json:"battery_pct"`
	Timestamp   int64   `json:"ts"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "QueueGo broker address")
	masterKey := flag.String("master-key", "my-master-key", "Broker master key")
	queueKey := flag.String("queue-key", "iot-telemetry-key", "Topic queue key")
	topicName := flag.String("topic", "telemetry", "Topic name")
	sensorID := flag.String("sensor", "sensor-node-01", "Sensor node identifier")
	location := flag.String("loc", "Warehouse-Section-B", "Sensor location")
	interval := flag.Duration("interval", 800*time.Millisecond, "Publishing interval")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*topicName)

	createToken, _ := auth.GenerateToken(*masterKey, *sensorID, *queueKey, "create")
	pubToken, _ := auth.GenerateToken(*masterKey, *sensorID, *queueKey, "publish")

	// Ensure topic exists
	_ = blink.SendFrame(conn, blink.NewCreateFrame([]byte(createToken), *topicName, 0x00))

	log.Printf("[%s] Starting IoT telemetry streaming to %q (ID: %d)...", *sensorID, *topicName, topicID)

	baseTemp := 21.0 + rand.Float64()*5.0
	baseHumidity := 45.0 + rand.Float64()*10.0
	battery := 100

	for {
		// Simulate fluctuating metrics
		currentTemp := baseTemp + (rand.Float64()*2.0 - 1.0)
		currentHumidity := baseHumidity + (rand.Float64()*4.0 - 2.0)
		if rand.Float64() < 0.1 && battery > 10 {
			battery--
		}

		telemetry := TelemetryData{
			SensorID:    *sensorID,
			Location:    *location,
			Temperature: currentTemp,
			Humidity:    currentHumidity,
			BatteryPct:  battery,
			Timestamp:   time.Now().Unix(),
		}

		data, _ := json.Marshal(telemetry)
		if err := blink.SendFrame(conn, blink.NewPublishFrame([]byte(pubToken), topicID, data)); err != nil {
			log.Fatalf("[%s] Failed to publish: %v", *sensorID, err)
		}

		fmt.Printf("\r[%s] Streamed -> Temp: %.1f°C | Humidity: %.1f%% | Battery: %d%%",
			*sensorID, currentTemp, currentHumidity, battery)

		time.Sleep(*interval)
	}
}

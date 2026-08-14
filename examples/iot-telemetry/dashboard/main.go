package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

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
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topicID := blink.HashTopic(*topicName)

	subToken, err := auth.GenerateToken(*masterKey, "telemetry-dashboard", *queueKey, "subscribe")
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}

	if err := blink.SendFrame(conn, blink.NewSubscribeFrame([]byte(subToken), topicID)); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	fmt.Printf("\n=========================================================================\n")
	fmt.Printf("           QueueGo IoT Real-Time Telemetry Monitor                      \n")
	fmt.Printf("=========================================================================\n")
	fmt.Printf("%-18s | %-20s | %-8s | %-8s | %-8s\n", "SENSOR ID", "LOCATION", "TEMP", "HUMIDITY", "BATTERY")
	fmt.Println("-------------------+----------------------+----------+----------+--------")

	msgCount := 0
	for {
		frame, err := blink.ReadFrame(conn)
		if err != nil {
			log.Fatalf("Disconnected from telemetry stream: %v", err)
		}

		if msg, ok := frame.(*blink.MessageFrame); ok {
			var tData TelemetryData
			if err := json.Unmarshal(msg.Payload, &tData); err == nil {
				msgCount++
				alert := ""
				if tData.Temperature > 25.0 {
					alert = " [HIGH TEMP]"
				}
				fmt.Printf("%-18s | %-20s | %6.1f°C | %6.1f%% | %6d%%%s\n",
					tData.SensorID, tData.Location, tData.Temperature, tData.Humidity, tData.BatteryPct, alert)
			}
		}
	}
}

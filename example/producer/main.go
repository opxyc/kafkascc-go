package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// Transaction represents a simulated banking action
type Transaction struct {
	TS          string  `json:"ts"`           // UTC timestamp
	Action      string  `json:"action"`       // e.g. money_transfer, balance_check
	UserID      string  `json:"user_id"`      // simulated user identifier
	AccountNo   string  `json:"account_no"`   // masked or partial account
	Amount      float64 `json:"amount"`       // transaction amount
	Currency    string  `json:"currency"`     // e.g. INR, USD
	Channel     string  `json:"channel"`      // mobile_app, web, atm
	DeviceModel string  `json:"device_model"` // e.g. iPhone 13, Pixel 8
	AppVersion  string  `json:"app_version"`  // simulated app version
	Location    string  `json:"location"`     // e.g. Kochi, Mumbai
	Status      string  `json:"status"`       // success, failed
}

func main() {
	broker := "localhost:9094" // external listener from docker-compose
	topic := "test-topic"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	fmt.Println("Producing banking events to Kafka topic:", topic)
	rand.Seed(time.Now().UnixNano())

	actions := []string{"money_transfer", "balance_check", "bill_payment", "upi_qr_scan"}
	statuses := []string{"success", "failed"}
	currencies := []string{"INR", "USD"}

	for i := 1; i <= 1000000; i++ {
		event := Transaction{
			TS:          time.Now().UTC().Format(time.RFC3339),
			Action:      actions[rand.Intn(len(actions))],
			UserID:      fmt.Sprintf("user_%03d", rand.Intn(999)),
			AccountNo:   fmt.Sprintf("XXXX-%04d", rand.Intn(9999)),
			Amount:      float64(rand.Intn(10000)) / 1.23,
			Currency:    currencies[rand.Intn(len(currencies))],
			Channel:     "mobile_app",
			DeviceModel: []string{"iPhone 13", "Pixel 8", "OnePlus 12"}[rand.Intn(3)],
			AppVersion:  fmt.Sprintf("v%d.%d.%d", rand.Intn(3)+1, rand.Intn(10), rand.Intn(5)),
			Location:    []string{"Mumbai", "Kochi", "Delhi", "Chennai"}[rand.Intn(4)],
			Status:      statuses[rand.Intn(len(statuses))],
		}

		value, _ := json.Marshal(event)

		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("txn-%d", i)),
				Value: value,
			},
		)
		if err != nil {
			log.Fatalf("Failed to write message: %v", err)
		}
		fmt.Println("Sent:", string(value))
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Println("✅ Done sending JSON messages")
}

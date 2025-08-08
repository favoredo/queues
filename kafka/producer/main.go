package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.AllowAutoTopicCreation(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	// Ensure we can connect
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("failed to ping kafka: %v", err)
	}

	log.Printf("connected to kafka at %s, producing to topic %s", brokers, topic)

	// Produce messages every second
	for i := 0; ; i++ {
		msg := fmt.Sprintf("message %d at %s", i, time.Now().Format(time.RFC3339))
		record := &kgo.Record{
			Topic: topic,
			Value: []byte(msg),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			log.Printf("failed to produce message: %v", err)
		} else {
			log.Printf("produced message: %s", msg)
		}

		time.Sleep(1 * time.Second)
	}
}

package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	log.Printf("connected to kafka at %s, consuming from topic %s in group %s", brokers, topic, groupID)

	for {
		fetches := client.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf("fetch error: %v", err)
			}
			continue
		}

		if fetches.IsClientClosed() {
			log.Printf("client closed :()")
			return
		}

		if fetches.NumRecords() == 0 {
			log.Printf("no records fetched")
			time.Sleep(1 * time.Second)
			continue
		}

		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			partition.EachRecord(func(record *kgo.Record) {
				log.Printf(
					"consumed message: partition=%d offset=%d key=%s value=%s",
					record.Partition, record.Offset, string(record.Key), string(record.Value),
				)
			})
		})
	}
}

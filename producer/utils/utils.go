package utils

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	brokers = "172.31.152.16:9092,172.31.152.16:9093,172.31.152.16:9094"
	topics  = "test-p3"
)

func Publish(message string) {
	fmt.Println("Hello Producer")

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "client1",
		"acks":              "all",
	}

	producer, err := kafka.NewProducer(config)

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
				return
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	newFunction(message, producer)

	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)

}

func newFunction(message string, producer *kafka.Producer) {
	// for _, word := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} {
	// 	producer.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{
	// 			Topic:     &topics,
	// 			Partition: kafka.PartitionAny,
	// 			Offset:    0,
	// 			Metadata:  new(string),
	// 			Error:     nil,
	// 		},
	// 		Value:         []byte(message),
	// 		Key:           []byte("key"),
	// 		Timestamp:     time.Time{},
	// 		TimestampType: 0,
	// 		Opaque:        nil,
	// 		Headers:       []kafka.Header{},
	// 	}, nil)
	// }

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topics,
			Partition: kafka.PartitionAny,
			Offset:    0,
			Metadata:  new(string),
			Error:     nil,
		},
		Value:         []byte(message),
		Key:           []byte{},
		Timestamp:     time.Time{},
		TimestampType: 0,
		Opaque:        nil,
		Headers:       []kafka.Header{},
	}, nil)
}

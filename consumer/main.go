package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	brokers = "172.31.152.16:9092,172.31.152.16:9093,172.31.152.16:9094"
	topics  = "test-p3"
)

func basic_pool(consumer *kafka.Consumer) {
	run := true
	for run == true {
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Println("Message Topic : " + string(*e.TopicPartition.Topic))
			fmt.Println("Message Patition : ", e.TopicPartition.Partition)
			fmt.Println("Message Offset : " + e.TopicPartition.Offset.String())
			fmt.Println("Message Value : " + string(e.Value))
			fmt.Println("####################################")
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
	consumer.Close()
}

func basic_sync_pool(consumer *kafka.Consumer) {
	run := true
	msg_count := 0
	for run == true {
		ev := consumer.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			msg_count += 1
			consumer.Commit()
			fmt.Println("Message Topic : " + string(*e.TopicPartition.Topic))
			fmt.Println("Message Patition : ", e.TopicPartition.Partition)
			fmt.Println("Message Offset : " + e.TopicPartition.Offset.String())
			fmt.Println("Message Value : " + string(e.Value))
			fmt.Println("Message key : " + string(e.Key))
			fmt.Println("####################################")
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = true
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
	consumer.Close()
}

func main() {
	fmt.Println("Hello Kafka")

	config := &kafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"group.id":                "myGroup",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      false,
		"auto.commit.interval.ms": 5000,
		"session.timeout.ms":      10000,
		"heartbeat.interval.ms":   3000,
	}

	consumer, err := kafka.NewConsumer(config)

	if err != nil {
		panic(err)
	}

	consumer.SubscribeTopics([]string{topics}, nil)

	//basic_pool(consumer)
	basic_sync_pool(consumer)

}

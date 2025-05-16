package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [produce|consume]")
		return
	}

	brokers := []string{"localhost:9095"}
	topic := "test-samara"

	switch os.Args[1] {
	case "produce":
		produce(brokers, topic)
	case "consume":
		consume(brokers, topic)
	default:
		fmt.Println("Unknown command:", os.Args[1])
	}
}

func produce(brokers []string, topic string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Producer error:", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("Message %d", i)),
		}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("Send error:", err)
		} else {
			fmt.Println("Produced:", msg.Value)
		}
		time.Sleep(1 * time.Second)
	}
}

func consume(brokers []string, topic string) {
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal("Consumer error:", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal("Partition error:", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatal("PartitionConsumer error:", err)
		}
		defer pc.Close()

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Consumed: %s\n", string(msg.Value))
			}
		}(pc)
	}

	// Wait for a while to consume messages
	time.Sleep(15 * time.Second)
}

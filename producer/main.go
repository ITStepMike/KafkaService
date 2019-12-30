package main

import (
	"fmt"
	"net/http"

	"github.com/Shopify/sarama"
)

var brokerPath = "kafka:9092"

func main() {
	fmt.Println(brokerPath)
	//addresses of available kafka brokers
	brokers := []string{brokerPath}
	//setup relevant config info
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "test2"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hi, hello"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	http.ListenAndServe(":8080", nil)
}

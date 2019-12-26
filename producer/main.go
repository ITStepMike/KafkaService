package main

import (
	"fmt"
	"net/http"
)

func main() {

	// p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	// if err != nil {
	// 	panic(err)
	// }

	// defer p.Close()

	// go func() {
	// 	for e := range p.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
	// 			} else {
	// 				fmt.Printf("Delivered message to %v\n", ev.TopicPatrition)
	// 			}
	// 		}
	// 	}
	// }()

	// topic := "myTopic"
	// for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
	// 	p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(word),
	// 	}, nil)
	// }

	// p.Flush(15 * 1000)

	fmt.Println("Server started at: 8080")
	http.ListenAndServe(":8080", nil)
}

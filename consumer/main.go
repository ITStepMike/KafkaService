package main

import (
	"fmt"
	"net/http"
)

func main() {

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers": "localhost",
	// 	"group.id":          "MyGroup",
	// 	"auto.offset.reset": "earliest",
	// })

	// defer c.Close()

	// if err != nil {
	// 	panic(err)
	// }

	// if err = c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil); err != nil {
	// 	fmt.Println("Can't subscribe to the topic because of ")
	// }

	// for {

	// 	msg, err := c.ReadMessage(1)
	// 	if err != nil {
	// 		fmt.Println("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	// 	} else {
	// 		fmt.Println("Consumer error: %v (%v)\n", err, msg)
	// 	}

	// }

	fmt.Println("Server started at: 8080")
	http.ListenAndServe(":8080", nil)
}

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/KafkaService/api/config"

	"github.com/KafkaService/api/models"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

//Producer for publishing
var Producer sarama.SyncProducer

//Consumer for subscribing
var Consumer *cluster.Consumer

//Config for flatteners communication with kafka
var Config models.FlattenersConfig

func main() {

	err := config.InitFlattenersConfig(&Config)
	if err != nil {
		panic(err)
	}

	setupProducer()

	defer func() {
		if err := Producer.Close(); err != nil {
			panic(err)
		}
	}()

	setupConsumer()

	defer Consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range Consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range Consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-Consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				var incomingMessage models.IncomingMessage

				err := json.NewDecoder(bytes.NewReader(msg.Value)).Decode(&incomingMessage)
				if err != nil {
					fmt.Printf("Error is %v\n", err)
					break
				}

				fmt.Printf("\n\n %v \n\n", incomingMessage)
				resp, err := formatIncomingMessage(&incomingMessage)
				if err != nil {
					fmt.Printf("Error is %v\n", err)
					break
				}

				fmt.Println(resp)

				message, err := json.Marshal(resp)
				if err != nil {
					fmt.Printf("Error is %v\n", err)
					break
				}
				for _, v := range Config.DestinationTopics {
					sendMessage(v, string(message))
					Consumer.MarkOffset(msg, "") // mark message as processed
				}
			}
		case <-signals:
			return
		}
	}

}

func setupProducer() {

	producerBrokers := []string{Config.BrokerAddress}
	//setup relevant config info
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(producerBrokers, producerConfig)
	if err != nil {
		panic(err)
	}

	Producer = producer

}

func setupConsumer() {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	brokers := []string{Config.BrokerAddress}
	topics := []string{"test1"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
	if err != nil {
		panic(err)
	}

	Consumer = consumer

}

//formatIncomingMessage formats incoming message from producer and seds it back to the destination topic
func formatIncomingMessage(incomingMessage *models.IncomingMessage) ([]models.DestinationMessage, error) {

	if incomingMessage == nil {
		return nil, errors.New("Incoming message is empty")
	}

	destinationMessages := make([]models.DestinationMessage, len(incomingMessage.Message.Partitions))

	for i := 0; i < len(incomingMessage.Message.Partitions); i++ {
		destinationMessages[i].Data.Name = incomingMessage.Message.Partitions[i].Name
		destinationMessages[i].Data.DriveType = incomingMessage.Message.Partitions[i].DriveType
		destinationMessages[i].Data.UsedSpaceBytes = incomingMessage.Message.Partitions[i].Metric.UsedSpaceBytes
		destinationMessages[i].Data.TotalSpaceBytes = incomingMessage.Message.Partitions[i].Metric.TotalSpaceBytes
		destinationMessages[i].Data.CreateAtTimeUTC = incomingMessage.Message.CreateAtTimeUTC
	}

	return destinationMessages, nil
}

func sendMessage(topic string, message string) {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := Producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n\n", topic, partition, offset)

}

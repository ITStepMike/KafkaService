package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/KafkaService/api/config"
	"github.com/KafkaService/api/models"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

//Producer for publishing
var Producer sarama.SyncProducer

//Consumer for subscribing
var Consumer *cluster.Consumer

//Config for flatteners communication with kafka
var Config models.FlattenersConfig

func main() {

	f, err := os.OpenFile("test.log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Error while setting log output to the file")
	}
	log.SetOutput(f)
	log.SetLevel(log.TraceLevel)

	if err := setupConfig(&Config); err != nil {
		log.Errorf("Setting up config failed with error: %v\n", err)
	}

	if err := setupProducer([]string{Config.BrokerAddress}); err != nil {
		log.Errorf("Setting up producer failed with error: %v\n", err)
	}

	defer func() {
		if err := Producer.Close(); err != nil {
			log.Infof("Connection with producer has been closed: %v\n", err)
		}
	}()

	if err := setupConsumer([]string{Config.BrokerAddress}); err != nil {
		log.Error(fmt.Sprintf("Setting up consumer failed with error: %v", err))
	}

	defer Consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range Consumer.Errors() {
			log.Errorf("Consumer error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range Consumer.Notifications() {
			log.Infof("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-Consumer.Messages():
			if ok {
				log.Infof("%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				var incomingMessage models.IncomingMessage

				log.Traceln("Decoding incoming message ...")
				err := json.NewDecoder(bytes.NewReader(msg.Value)).Decode(&incomingMessage)
				if err != nil {
					log.Warnf("Error while decoding incoming message: %v\n", err)
					break
				}
				log.Tracef("Incoming message has beed decoded properly: %v\n", incomingMessage)

				log.Traceln("Formatting incoming message to destination message ...")
				resp, err := formatIncomingMessage(&incomingMessage)
				if err != nil {
					log.Warnf("Error while formatting incoming message: %v\n", err)
					break
				}
				log.Tracef("Formatting incoming message to destination message finished: %v\n", incomingMessage)

				log.Traceln("Formatting destination message before sending to the kafka broker ...")
				message, err := json.Marshal(resp)
				if err != nil {
					log.Warnf("Error while formatting destination message: %v\n", err)
					break
				}
				log.Traceln("Formatting destination message finished")

				log.Traceln("Sending messages to the kafka")
				for _, v := range Config.DestinationTopics {
					sendMessage(v, string(message))
				}
				Consumer.MarkOffset(msg, "") // mark message as processed
				log.Traceln("Sending has been finished")
			}
		case <-signals:
			return
		}
	}

}

func setupConfig(flatterConfig *models.FlattenersConfig) error {

	err := config.InitFlattenersConfig(flatterConfig)
	if err != nil {
		return err
	}

	return nil

}

func setupProducer(brokerAddress []string) error {

	producerBrokers := brokerAddress
	//setup relevant config info
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(producerBrokers, producerConfig)
	if err != nil {
		return err
	}

	Producer = producer

	return nil
}

func setupConsumer(brokerAddress []string) error {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	brokers := brokerAddress
	topics := []string{"test1"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
	if err != nil {
		return err
	}

	Consumer = consumer

	return nil
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
		fmt.Println(err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n\n", topic, partition, offset)

}

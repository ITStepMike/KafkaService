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

//FlattenersConfig for flatteners communication with kafka
var FlattenersConfig models.FlattenersConfig

//LogConfig for logging
var LogConfig models.LogConfig

func main() {

	log.Traceln("Setting up logging config...")
	if err := config.InitLogConfig(&LogConfig); err != nil {
		log.Errorf("Setting up log config failed with error: %v\n", err)
	}
	log.Traceln("Setting up logging config finished")

	log.Traceln("Opening logging file...")
	f, err := startLogging(&LogConfig)
	if err != nil {
		log.Errorf("Starting logging failed with error: %v\n", err)
	}
	defer f.Close()
	log.Traceln("Log file has been opened")

	log.Traceln("Setting up flatteners config...")
	if err := config.InitFlattenersConfig(&FlattenersConfig); err != nil {
		log.Errorf("Setting up flatteners config failed with error: %v\n", err)
	}
	log.Traceln("Setting up flatteners config finished")

	log.Traceln("Setting up producer...")
	if err := setupProducer([]string{FlattenersConfig.BrokerAddress}, &Producer); err != nil {
		log.Errorf("Setting up producer failed with error: %v\n", err)
	}

	defer func() {
		if err := Producer.Close(); err != nil {
			log.Infof("Connection with producer has been closed: %v\n", err)
		}
	}()
	log.Traceln("Producer has been started")

	log.Traceln("Setting up Consumer...")
	if err := setupConsumer([]string{FlattenersConfig.BrokerAddress}, FlattenersConfig.InputTopics, &Consumer); err != nil {
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
	log.Traceln("Consumer has been started")

	log.Traceln("Waiting for messages...")
	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-Consumer.Messages():
			if ok {
				log.Tracef("New incoming message")
				log.Infof("%s/%d/%d\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key)
				var incomingMessage models.IncomingMessage

				log.Traceln("Decoding incoming message...")
				err := json.NewDecoder(bytes.NewReader(msg.Value)).Decode(&incomingMessage)
				if err != nil {
					log.Warnf("Error while decoding incoming message: %v\n", err)
					break
				}
				log.Tracef("Incoming message has beed decoded properly: %v\n", incomingMessage)

				log.Traceln("Formatting incoming message to destination message...")
				resp, err := formatIncomingMessage(&incomingMessage)
				if err != nil {
					log.Warnf("Error while formatting incoming message: %v\n", err)
					break
				}
				log.Tracef("Formatting incoming message to destination message finished: %v\n", incomingMessage)

				log.Traceln("Formatting destination message before sending to the kafka broker...")
				message, err := json.Marshal(resp)
				if err != nil {
					log.Warnf("Error while formatting destination message: %v\n", err)
					break
				}
				log.Traceln("Formatting destination message finished")

				log.Traceln("Sending messages to the kafka")
				for _, v := range FlattenersConfig.DestinationTopics {
					if err = sendMessage(v, string(message)); err != nil {
						log.Error(err.Error())
					}
				}
				Consumer.MarkOffset(msg, "") // mark message as processed
				log.Traceln("Sending has been finished")
			}
		case <-signals:
			return
		}
	}

}

func startLogging(logConfig *models.LogConfig) (*os.File, error) {

	log.Traceln("Logs file is opening...")
	f, err := os.OpenFile(logConfig.LogFilePath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	log.Traceln("Logs file is has been opend")

	log.SetOutput(f)

	var logLevel string

	switch logConfig.LogLevel {
	case "Trace":
		logLevel = "Trace"
		log.SetLevel(log.TraceLevel)
	case "Debug":
		logLevel = "Debug"
		log.SetLevel(log.DebugLevel)
	case "Info":
		logLevel = "Info"
		log.SetLevel(log.InfoLevel)
	case "Warn":
		logLevel = "Warn"
		log.SetLevel(log.WarnLevel)
	case "Error":
		logLevel = "Error"
		log.SetLevel(log.ErrorLevel)
	case "Fatal":
		logLevel = "Fatal"
		log.SetLevel(log.FatalLevel)
	case "Panic":
		logLevel = "Panic"
		log.SetLevel(log.PanicLevel)
	default:
		logLevel = "Info"
		log.SetLevel(log.InfoLevel)
	}
	log.Infof("Log level has been set to %s", logLevel)

	return f, err
}

func setupProducer(brokerAddress []string, producer *sarama.SyncProducer) error {

	if producer == nil {
		return errors.New("Producer value is empty")
	}

	producerBrokers := brokerAddress
	//setup relevant config info
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	log.Traceln("\tCreating sync producer...")
	newProducer, err := sarama.NewSyncProducer(producerBrokers, producerConfig)
	if err != nil {
		return err
	}

	*producer = newProducer
	log.Traceln("\tSync producer has been created")

	return nil
}

func setupConsumer(brokerAddress []string, incomingTopics []string, consumer **cluster.Consumer) error {

	if consumer == nil {
		return errors.New("Consumer value is empty")
	}

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	log.Traceln("\tCreating consumer...")
	newConsumer, err := cluster.NewConsumer(brokerAddress, "my-consumer-group", incomingTopics, config)
	if err != nil {
		return err
	}

	*consumer = newConsumer
	log.Traceln("\tConsumer has been created")

	return nil
}

//formatIncomingMessage formats incoming message from producer and seds it back to the destination topic
func formatIncomingMessage(incomingMessage *models.IncomingMessage) ([]models.DestinationMessage, error) {

	if incomingMessage == nil {
		return nil, errors.New("Incoming message is empty")
	}

	destinationMessages := make([]models.DestinationMessage, 0)

	for i := 0; i < len(incomingMessage.Message.Partitions); i++ {
		destinationMessages = append(destinationMessages, models.DestinationMessage{
			Data: models.Data{
				Name:            incomingMessage.Message.Partitions[i].Name,
				DriveType:       incomingMessage.Message.Partitions[i].DriveType,
				UsedSpaceBytes:  incomingMessage.Message.Partitions[i].Metric.UsedSpaceBytes,
				TotalSpaceBytes: incomingMessage.Message.Partitions[i].Metric.TotalSpaceBytes,
				CreateAtTimeUTC: incomingMessage.Message.CreateAtTimeUTC,
			},
		})
	}

	return destinationMessages, nil
}

func sendMessage(topic string, message string) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	log.Traceln("\tSending message...")
	partition, offset, err := Producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Infof("\tMessage is stored in topic(%s)/partition(%d)/offset(%d)\n\n", topic, partition, offset)

	return nil
}

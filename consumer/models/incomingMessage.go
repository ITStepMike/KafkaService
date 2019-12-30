package models

//IncomingMessage from kafka
type IncomingMessage struct {
	InputTopic       string  `json:"inputTopic"`
	DestinationTopic string  `json:"destinationTopic"`
	Action           string  `json:"Action"`
	Message          Message `json:"Message"`
}

package models

//IncomingMessage from kafka
type IncomingMessage struct {
	Action  string  `json:"Action"`
	Message Message `json:"Message"`
}

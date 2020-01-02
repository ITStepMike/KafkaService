package models

//Message inside IncomingMessage
type Message struct {
	Partitions      []Partition `json:"partitions"`
	CreateAtTimeUTC string      `json:"createAtTimeUTC"`
}

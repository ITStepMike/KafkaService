package models

//Message inside IncomingMessage
type Message struct {
	Partitions       []Partition `json:"partitions"`
	CreatedAtTimeUTC string      `json:"createAtTimeUTC"`
}

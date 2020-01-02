package models

//Partition for Message obj
type Partition struct {
	Name      string `json:"name"`
	DriveType int64  `json:"driveType"`
	Metric    Metric `json:"metric"`
}

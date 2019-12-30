package models

//Partition for Message obj
type Partition struct {
	Name      string `json:"name"`
	DriveType string `json:"driveType"`
	Metric    Metric `json:"metric"`
}

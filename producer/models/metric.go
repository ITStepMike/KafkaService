package models

//Metric inside partition
type Metric struct {
	UsedSpaceBytes  int64 `json:"usedSpaceBytes"`
	TotalSpaceBytes int64 `json:"totalSpaceBytes"`
}

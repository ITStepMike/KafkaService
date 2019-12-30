package models

//Data inside destination message
type Data struct {
	Name            string `json:"name"`
	DriveType       int64 `json:"driveType"`
	UsedSpaceBytes  int64  `json:"usedSpaceBytes"`
	TotalSpaceBytes int64  `json:"totalSpaceBytes"`
	CreateAtTimeUTC string `json:"createAtTimeUTC"`
}

package models

//FlattenersConfig for kafka connection
type FlattenersConfig struct {
	BrokerAddress     string   `json:"brokerAddress"`
	InputTopics       []string `json:"inputTopics"`
	DestinationTopics []string `json:"destinationTopics"`
}

//LogConfig for logging information to the logfile
type LogConfig struct {
	LogFilePath string `json:"logFilePath"`
	LogLevel    string `json:"logLevel"`
}

package models

type FlattenersConfig struct {
	BrokerAddress     string   `yaml:"brokerAddress"`
	InputTopics       []string `yaml:"inputTopics"`
	DestinationTopics []string `yaml:"destinationTopics"`
}

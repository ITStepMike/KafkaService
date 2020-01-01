package config

import (
	"io/ioutil"
	"path/filepath"

	"github.com/KafkaService/consumer/models"
	"gopkg.in/yaml.v2"
)

var flattenersConfigPath = "./config/flattenersConfig.yml"

//InitFlattenersConfig read basic flattener's config with input and destination topics
func InitFlattenersConfig(config *models.FlattenersConfig) error {

	configFile, err := filepath.Abs(flattenersConfigPath)
	if err != nil {
		return err
	}

	yamlConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(yamlConfig, config)
	if err != nil {
		return err
	}

	return nil
}

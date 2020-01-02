package config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/KafkaService/api/models"
)

var flattenersConfigPath = "./config/flattenersConfig.json"

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

	err = json.Unmarshal(yamlConfig, config)
	if err != nil {
		return err
	}

	return nil
}

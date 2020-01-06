package config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/KafkaService/api/models"
	log "github.com/sirupsen/logrus"
)

//InitFlattenersConfig read basic flattener's config with input and destination topics
func InitFlattenersConfig(flattenersConfigPath string, config *models.FlattenersConfig) error {

	log.Traceln("Setting up flatteners config...")

	configFile, err := filepath.Abs(flattenersConfigPath)
	if err != nil {
		return err
	}

	jsonConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonConfig, config)
	if err != nil {
		return err
	}
	log.Traceln("Setting up flatteners config finished")

	return nil
}

//InitLogConfig read basic log config with configFilePath and logging level
func InitLogConfig(logConfigPath string, config *models.LogConfig) error {

	log.Traceln("Setting up logging config...")
	configFile, err := filepath.Abs(logConfigPath)
	if err != nil {
		return err
	}

	jsonConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonConfig, config)
	if err != nil {
		return err
	}
	log.Traceln("Setting up logging config finished")

	return nil
}

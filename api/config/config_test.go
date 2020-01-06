package config

import (
	"os"
	"testing"

	"github.com/KafkaService/api/models"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

func TestInitLogConfigOK(t *testing.T) {

	flagsters := struct {
		inPath    string
		inString  string
		outConfig *models.LogConfig
	}{
		"./logConfigTest.json",
		`{
			"logFilePath" : "./logFile.log",
			"logLevel" : "Trace"
		}`,
		&models.LogConfig{
			LogFilePath: "./logFile.log",
			LogLevel:    "Trace",
		},
	}

	var _, err = os.Stat(flagsters.inPath)

	var file *os.File
	if os.IsNotExist(err) {
		file, err = os.Create(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigOK: %v", err)
		}

	} else {
		file, err = os.OpenFile(flagsters.inPath, os.O_RDWR, 0644)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigOK: %v", err)
		}
	}

	defer func() {
		err = os.Remove(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigOK: %v", err)
		}
	}()

	defer file.Close()

	file.WriteString(flagsters.inString)

	config := &models.LogConfig{}

	err = InitLogConfig(flagsters.inPath, config)

	assert.Nil(t, err)

	assert.Equal(t, flagsters.outConfig, config)
}

func TestInitLogConfigFilePathError(t *testing.T) {

	flagsters := struct {
		inPath    string
		outError  string
		outConfig *models.LogConfig
	}{
		"./logConfigTest.json",
		"no such file or directory",
		&models.LogConfig{},
	}

	config := &models.LogConfig{}

	err := InitLogConfig(flagsters.inPath, config)

	assert.Equal(t, flagsters.outError, err.Error()[len(err.Error())-25:])

	assert.Equal(t, flagsters.outConfig, config)

}

func TestInitLogConfigUnmarshalError(t *testing.T) {

	flagsters := struct {
		inPath    string
		inString  string
		outConfig *models.LogConfig
	}{
		"./logConfigTest.json",
		`{"logFilePath" : "./logFile.log"`,
		&models.LogConfig{},
	}

	var _, err = os.Stat(flagsters.inPath)

	var file *os.File
	if os.IsNotExist(err) {
		file, err = os.Create(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}

	} else {
		file, err = os.OpenFile(flagsters.inPath, os.O_RDWR, 0644)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}
	}

	defer func() {
		err = os.Remove(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}
	}()

	defer file.Close()

	file.WriteString(flagsters.inString)

	config := &models.LogConfig{}

	err = InitLogConfig(flagsters.inPath, config)

	assert.NotNil(t, err)

	assert.Equal(t, flagsters.outConfig, config)
}

func TestInitFlatterConfigOK(t *testing.T) {

	flagsters := struct {
		inPath    string
		inString  string
		outConfig *models.FlattenersConfig
	}{
		"./flatterConfigTest.json",
		`{
			"brokerAddress" : "localhost:9092",
			"inputTopics" : [
				"test1"
			],
			"destinationTopics" : [
				"test2",
				"test3"
			]
		}`,
		&models.FlattenersConfig{
			BrokerAddress: "localhost:9092",
			InputTopics: []string{
				"test1",
			},
			DestinationTopics: []string{
				"test2",
				"test3",
			},
		},
	}

	var _, err = os.Stat(flagsters.inPath)

	var file *os.File
	if os.IsNotExist(err) {
		file, err = os.Create(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitFlatterConfigOK: %v", err)
		}

	} else {
		file, err = os.OpenFile(flagsters.inPath, os.O_RDWR, 0644)
		if err != nil {
			log.Errorf("Error while TestInitFlatterConfigOK: %v", err)
		}
	}

	defer func() {
		err = os.Remove(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitFlatterConfigOK: %v", err)
		}
	}()

	defer file.Close()

	file.WriteString(flagsters.inString)

	config := &models.FlattenersConfig{}

	err = InitFlattenersConfig(flagsters.inPath, config)

	assert.Nil(t, err)

	assert.Equal(t, flagsters.outConfig, config)
}

func TestInitFlatterConfigFilePathError(t *testing.T) {

	flagsters := struct {
		inPath    string
		outError  string
		outConfig *models.FlattenersConfig
	}{
		"./flatterConfigTest.json",
		"no such file or directory",
		&models.FlattenersConfig{},
	}

	config := &models.FlattenersConfig{}

	err := InitFlattenersConfig(flagsters.inPath, config)

	assert.Equal(t, flagsters.outError, err.Error()[len(err.Error())-25:])

	assert.Equal(t, &models.FlattenersConfig{}, config)

}

func TestInitFlatterConfigUnmarshalError(t *testing.T) {

	flagsters := struct {
		inPath    string
		inString  string
		outConfig *models.FlattenersConfig
	}{
		"./flatterConfigTest.json",
		`{,
			"inputTopics" : [
				"test1"
			],
			"destinationTopics" : [
				"test2",
		}`,
		&models.FlattenersConfig{},
	}

	var _, err = os.Stat(flagsters.inPath)

	var file *os.File
	if os.IsNotExist(err) {
		file, err = os.Create(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}

	} else {
		file, err = os.OpenFile(flagsters.inPath, os.O_RDWR, 0644)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}
	}

	defer func() {
		err = os.Remove(flagsters.inPath)
		if err != nil {
			log.Errorf("Error while TestInitLogConfigUnmarshalError: %v", err)
		}
	}()

	defer file.Close()

	file.WriteString(flagsters.inString)

	config := &models.FlattenersConfig{}

	err = InitFlattenersConfig(flagsters.inPath, config)

	assert.NotNil(t, err)

	assert.Equal(t, flagsters.outConfig, config)
}

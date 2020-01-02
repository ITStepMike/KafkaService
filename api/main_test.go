package main

import (
	"testing"

	"github.com/KafkaService/api/models"
	"github.com/stretchr/testify/assert"
)

func TestFormatIncomingMessageOK(t *testing.T) {
	var incomingMessageOK = models.IncomingMessage{
		DestinationTopic: "destinationTopic",
		InputTopic:       "inputTopic",
		Action:           "something",
		Message: models.Message{
			Partitions: []models.Partition{
				models.Partition{
					Name:      "c",
					DriveType: 3,
					Metric: models.Metric{
						UsedSpaceBytes:  342734824,
						TotalSpaceBytes: 34273482423,
					},
				},
				models.Partition{
					Name:      "d",
					DriveType: 3,
					Metric: models.Metric{
						UsedSpaceBytes:  942734824,
						TotalSpaceBytes: 904273482423,
					},
				},
			},
			CreateAtTimeUTC: "2017-08-07T08:38:43.3059476Z",
		},
	}

	destinationMessagesOK := []models.DestinationMessage{
		models.DestinationMessage{
			Data: models.Data{
				Name:            "c",
				DriveType:       3,
				UsedSpaceBytes:  342734824,
				TotalSpaceBytes: 34273482423,
				CreateAtTimeUTC: "2017-08-07T08:38:43.3059476Z",
			},
		},
		models.DestinationMessage{
			Data: models.Data{
				Name:            "d",
				DriveType:       3,
				UsedSpaceBytes:  942734824,
				TotalSpaceBytes: 904273482423,
				CreateAtTimeUTC: "2017-08-07T08:38:43.3059476Z",
			},
		},
	}

	destinationMessages, err := formatIncomingMessage(&incomingMessageOK)

	assert.Nil(t, err)

	assert.Equal(t, destinationMessages, destinationMessagesOK)

}

func TestFormatIncomingMessageNilError(t *testing.T) {

	_, err := formatIncomingMessage(nil)

	assert.NotEqual(t, err, nil)

	assert.Equal(t, err.Error(), "Incoming message is empty")

}

func TestSetupConfigOK(t *testing.T) {

	config := models.FlattenersConfig{}

	err := setupConfig(&config)

	assert.Nil(t, err)

}

func TestSetupConfigError(t *testing.T) {

	err := setupConfig(nil)

	assert.NotEqual(t, err, nil)

}

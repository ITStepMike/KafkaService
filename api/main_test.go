package main

import (
	"errors"
	"testing"

	"github.com/KafkaService/api/models"
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
	if err != nil {
		t.Errorf("Formatting message is incorrect got: %v, want: %v", err, nil)
		return
	}

	if len(destinationMessages) != len(destinationMessagesOK) {
		t.Errorf("Formatting message is incorrect (length is different)\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
		return
	}

	for k := 0; k < len(destinationMessagesOK); k++ {
		switch {
		case destinationMessages[k].Data.Name != destinationMessagesOK[k].Data.Name:
			t.Errorf("Formatting message incorrect\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
			return
		case destinationMessages[k].Data.DriveType != destinationMessagesOK[k].Data.DriveType:
			t.Errorf("Formatting message incorrect\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
			return
		case destinationMessages[k].Data.UsedSpaceBytes != destinationMessagesOK[k].Data.UsedSpaceBytes:
			t.Errorf("Formatting message incorrect\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
			return
		case destinationMessages[k].Data.TotalSpaceBytes != destinationMessagesOK[k].Data.TotalSpaceBytes:
			t.Errorf("Formatting message incorrect\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
			return
		case destinationMessages[k].Data.CreateAtTimeUTC != destinationMessagesOK[k].Data.CreateAtTimeUTC:
			t.Errorf("Formatting message incorrect\n  got: %v,\n want: %v", destinationMessages, destinationMessagesOK)
			return
		}
	}

}

func TestFormatIncomingMessageNilError(t *testing.T) {

	_, err := formatIncomingMessage(nil)
	if err == nil {
		t.Errorf("Formatting message is incorrect got: %v, want: %v", err, errors.New("Incoming message is empty"))
		return
	} else if err != nil && err.Error() != "Incoming message is empty" {
		t.Errorf("Formatting message is incorrect got: %v, want: %v", err, errors.New("Incoming message is empty"))
		return
	}

}

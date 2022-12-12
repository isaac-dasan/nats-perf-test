package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type (
	// PerfMsg is a message used for performance testing
	PerfMsg struct {
		Subject string
		Data    []byte
		Created int64
	}
)

// CreateID returns the first part of a uuid
func CreateID() string {
	return strings.Split(uuid.New().String(), "-")[0]
}

// CreateMessage creates a message with a given subject and data
func CreateMessage(subject string, data []byte) PerfMsg {
	return PerfMsg{
		Subject: CreateID(),
		Data:    data,
		Created: time.Now().UnixNano(),
	}
}

func GetSubjectName(streamName string, consumerNum int, subjectNum int) string {
	return fmt.Sprintf("%s.consumer-%d.subject-%d", streamName, consumerNum, subjectNum)
}

// createStream creates a stream with a number of subjects
func createStream(
	nc *nats.Conn,
	streamName string,
	subjectCount int,
	streamMaxMsgs int64,
	storageType string,
	replicas int,
	maxAgeMins int,
) ([]string, error) {
	fmt.Println("Starting createStream")
	subjects := []string{fmt.Sprintf("%s.>", streamName)}

	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
		Storage:  nats.MemoryStorage,
		MaxAge:   time.Duration(maxAgeMins) * time.Minute,
		Discard:  nats.DiscardOld,
		Replicas: replicas,
	}
	if storageType == "file" {
		cfg.Storage = nats.FileStorage
	} else {
		cfg.Storage = nats.MemoryStorage
		cfg.MaxMsgs = streamMaxMsgs // To limit number of messages in memory
	}
	// Create JetStream Context
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))

	if err != nil {
		publishFailureCount.WithLabelValues("Get Jetstream Context Error").Inc()
		fmt.Println("Error getting jetstream context :", err, " -> ", streamName)
		return subjects, err
	}
	fmt.Println("Recieved Jetstream context!")

	_, err = js.AddStream(cfg)
	if err != nil {
		publishFailureCount.WithLabelValues("AdddStream Error").Inc()
		fmt.Println("Error adding stream :", err, " -> ", streamName)
		return make([]string, 0), err
	}
	fmt.Printf("Create Stream succeeded: %s , with subjects - %s", streamName, subjects)
	fmt.Println()
	return subjects, nil
}

// removeStream - deletes a stream
func removeStream(nc *nats.Conn, streamName string) error {
	// Create JetStream Context
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))

	if err != nil {
		publishFailureCount.WithLabelValues("Get Jetstream Context Error").Inc()
		fmt.Println("Error getting jetstream context :", err, " -> ", streamName)
		return err
	}
	fmt.Println("Recieved Jetstream context!")
	err = js.DeleteStream(streamName)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			publishFailureCount.WithLabelValues("Delete Stream Error").Inc()
			fmt.Println("Error deleting old stream :", err, " -> ", streamName)
			fmt.Println("Waiting 30 secs before creating new stream.")
			ticker := time.NewTicker(time.Second * time.Duration(30))
			<-ticker.C // wait for old stream to be cleaned up
			return err
		}
		fmt.Println("Stream not found!")
		return nil
	}
	fmt.Println("Stream deleted successfully!")
	return nil
}

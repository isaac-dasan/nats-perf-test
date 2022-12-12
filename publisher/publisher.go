package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var publishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "db_publish_response_time_seconds",
	Help: "Duration of publish per requests.",
}, []string{})
var publishCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "db_publish_count",
	Help: "count of publish",
}, []string{})
var publishFailureCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "db_publish_failure_count",
	Help: "Count of failed publish requests.",
}, []string{"Error"})

func publish(nc *nats.Conn, streamName string, subject string, data []byte, guard chan struct{}) {
	// fmt.Println("Temp log: starting publish")
	err := sendMessage(nc, streamName, subject, data)
	if err != nil {
		fmt.Println("Publish error:", err)
		publishFailureCount.WithLabelValues("Publish error").Inc()
	}
	<-guard
}

// SendMessage sends a message to a given subject
func sendMessage(nc *nats.Conn, streamName string, subject string, data []byte) error {
	// fmt.Println("Temp log: starting sendMessage")
	msg := CreateMessage(subject, data)
	json, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	timer := prometheus.NewTimer(publishDuration.WithLabelValues())
	// Create JetStream Context
	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))

	if err != nil {
		publishFailureCount.WithLabelValues("Get Jetstream Context Error").Inc()
		fmt.Println("Error getting jetstream context :", err, " -> ", streamName)
		return err
	}
	_, err = js.Publish(subject, json, nats.ExpectStream(streamName))
	// fmt.Println("Temp log: Message published!")
	if err != nil {
		return err
	}
	timer.ObserveDuration()
	publishCount.WithLabelValues().Inc()
	return nil
}

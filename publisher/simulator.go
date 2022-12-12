package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	SimulationRequest struct {
		nc         *nats.Conn
		subject    string
		streamName string
		data       []byte
	}

	Observer struct {
		// simulationRequest Channel
		reqChannel chan *SimulationRequest
	}
)

func newSimulationRequest(nc *nats.Conn, subject string, streamName string, data []byte) *SimulationRequest {
	return &SimulationRequest{
		nc:         nc,
		subject:    subject,
		streamName: streamName,
		data:       data,
	}
}

func newObserver(maxRequestQueueDepth int64) *Observer {
	fmt.Println("New Observer created!")
	return &Observer{
		reqChannel: make(chan *SimulationRequest, maxRequestQueueDepth),
	}
}

var bulkPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "db_bulk_publish_response_time_seconds",
	Help:    "Duration of publish bulk requests.",
	Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 200},
}, []string{})
var bulkPublishCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "db_bulk_publish_count",
	Help: "count of bulk publish",
}, []string{})
var publishQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "db_publish_queue_depth",
	Help: "count of request in publish queue",
})

// listens to simulation request unless canceled
func listenForPublishRequest(ctx context.Context, obs *Observer, maxGoroutines int64) {
	fmt.Println("Starting listenForPublishRequest")
	guard := make(chan struct{}, maxGoroutines)
	for {
		select {
		case <-ctx.Done():
			return
		case simReq := <-obs.reqChannel:
			// fmt.Println("Temp log: Received publish request")
			guard <- struct{}{} // would block if guard channel is already filled
			go publish(simReq.nc, simReq.streamName, simReq.subject, simReq.data, guard)
		}
	}
}

func (obs *Observer) queuePublishRequest(ctx context.Context, nc *nats.Conn, streamName string, subjectCount int, messageCount int, consumerCount int, data []byte) {
	// fmt.Println("Temp log: Starting queuePublishRequest")
	// fmt.Printf("streamName - %s, subjects - %d, messageCount - %d", streamName, subjectCount, messageCount)
	// fmt.Println()
	timer := prometheus.NewTimer(bulkPublishDuration.WithLabelValues())
	currMessageCount := 0
	currConsumerCount := 0
	curSubjectNum := 0

	for currMessageCount < int(messageCount) {
		// fmt.Println("Temp log: Inside message count loop")
		select {
		case <-ctx.Done():
			return
		default:
			// fmt.Println("Temp log: Defaulting queuePublishRequest")
			obs.reqChannel <- newSimulationRequest(nc, GetSubjectName(streamName, currConsumerCount, curSubjectNum), streamName, data)
			currMessageCount++
			currConsumerCount++
			curSubjectNum++
			if currConsumerCount == consumerCount {
				currConsumerCount = 0
			}
			if curSubjectNum == subjectCount {
				curSubjectNum = 0
			}
		}
	}
	timer.ObserveDuration()
	bulkPublishCount.WithLabelValues().Inc()
	// fmt.Println("Temp log: Ending queuePublishRequest")
}

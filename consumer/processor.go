package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var msgDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "db_msg_travel_time_seconds",
	Help:    "Duration of message to come accross nats jetstream.",
	Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30, 60, 120, 240, 480, 960},
}, []string{})
var msgCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "db_msg_count",
	Help: "count of messages",
}, []string{})

type (
	Processor struct {
		context     context.Context
		cancel      context.CancelFunc
		nc          *nats.Conn
		latencyChan chan time.Duration
	}
)

func newProcessor(ctx context.Context, nc *nats.Conn) *Processor {
	context, cancel := context.WithCancel(ctx)
	latencyChanelSize, _ := strconv.ParseInt(os.Getenv("latencyChanelSize"), 0, 64)
	return &Processor{
		cancel:      cancel,
		context:     context,
		nc:          nc,
		latencyChan: make(chan time.Duration, int(latencyChanelSize)),
	}
}

func (p *Processor) start() {
	consumerBatchSize, _ := strconv.ParseInt(os.Getenv("consumerBatchSize"), 0, 64)
	consumerPullMaxWaiting, _ := strconv.ParseInt(os.Getenv("consumerPullMaxWaiting"), 0, 64)
	// subscriptionsPerConsumer, _ := strconv.ParseInt(os.Getenv("subscriptionsPerConsumer"), 0, 64)
	subjectsPerConsumer, _ := strconv.ParseInt(os.Getenv("subjectsPerConsumer"), 0, 64)
	consumerWithWildCard, _ := strconv.ParseBool(os.Getenv("consumerWithWildCard"))
	consumerPersistence := os.Getenv("consumerPersistence")
	subjectCount, _ := strconv.ParseInt(os.Getenv("subjectCount"), 0, 64)
	streamNamePrefix := os.Getenv("streamNamePrefix")
	curPodNumber := strings.Split(os.Getenv("MY_POD_NAME"), "-")[2]
	curStreamName := streamNamePrefix + "-" + curPodNumber
	podNumber, _ := strconv.ParseInt(curPodNumber, 0, 64)

	consumerCount := int(subjectCount) / int(subjectsPerConsumer)

	if int(podNumber) != 0 {
		ticker := time.NewTicker(time.Second * time.Duration(30*podNumber))
		fmt.Println("Waiting to prevent thundering herd problem becuase of replicaSet startup.")
		<-ticker.C // Initial wait for timer to avoid thundering herd issue
	}

	if consumerWithWildCard {
		for curConsmerCount := 0; curConsmerCount < int(consumerCount); curConsmerCount++ {
			ticker := time.NewTicker(time.Millisecond * time.Duration(10))
			<-ticker.C // wait for timer to avoid thundering herd issue
			ctx, _ := context.WithCancel(p.context)
			curConsumer := newConsumer(ctx, p.nc, curStreamName, GetConsumerName(curStreamName, curConsmerCount), GetSubjectWildCard(curStreamName, curConsmerCount), &p.latencyChan)
			go curConsumer.startConsumer(int(consumerPullMaxWaiting), int(consumerBatchSize), consumerPersistence)
		}
	} else {
		// No way to have multiple subejects to 1 consumer. Only option is 1:1
		for curSubjectCount := 0; curSubjectCount < int(consumerCount); curSubjectCount++ {
			ticker := time.NewTicker(time.Millisecond * time.Duration(10))
			<-ticker.C // wait for timer to avoid thundering herd issue
			ctx, _ := context.WithCancel(p.context)
			curConsumer := newConsumer(ctx, p.nc, curStreamName, GetConsumerName(curStreamName, curSubjectCount), GetSubjectName(curStreamName, curSubjectCount, curSubjectCount), &p.latencyChan)
			go curConsumer.startConsumer(int(consumerPullMaxWaiting), int(consumerBatchSize), consumerPersistence)
		}
	}
}

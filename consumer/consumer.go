package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var errorCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "db_consumer_error_count",
	Help: "count of errors",
}, []string{"ErrorType"})

type (
	Consumer struct {
		context       context.Context
		nc            *nats.Conn
		streamName    string
		consumerName  string
		filterSubject string
		latencyChan   chan time.Duration
	}
)

func newConsumer(context context.Context,
	nc *nats.Conn,
	streamName string,
	consumerName string,
	filterSubject string,
	latencyChan *chan time.Duration) *Consumer {
	return &Consumer{
		context:       context,
		nc:            nc,
		streamName:    streamName,
		consumerName:  consumerName,
		filterSubject: filterSubject,
		latencyChan:   *latencyChan,
	}
}

func (c *Consumer) startConsumer(consumerPullMaxWaiting int,
	consumerBatchSize int,
	consumerPersistence string) error {
	memoryStorage := false
	if consumerPersistence == "memory" {
		memoryStorage = true
	}

	// Create JetStream Context
	js, err := c.nc.JetStream(nats.MaxWait(30 * time.Second))

	if err != nil {
		errorCount.WithLabelValues("Get Jetstream Context Error").Inc()
		fmt.Println("Error getting jetstream context :", err, " -> ", c.streamName, " -> ", c.consumerName)
		return err
	}
	_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
		Durable:       c.consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverNewPolicy,
		FilterSubject: c.filterSubject,
		MemoryStorage: memoryStorage,
		MaxWaiting:    consumerPullMaxWaiting,
	})
	if err != nil {
		errorCount.WithLabelValues("Consumer creation Error").Inc()
		fmt.Println("Error creating consumer :", err, " -> ", c.streamName, " -> ", c.consumerName)
		return err
	}
	defer js.DeleteConsumer(c.streamName, c.consumerName)

	ctx, _ := context.WithCancel(c.context)
	err = c.startSubscribers(ctx, consumerBatchSize)
	if err != nil {
		errorCount.WithLabelValues("Subscription create Error").Inc()
		fmt.Println("Error creating subscription :", err, " -> ", c.streamName, " -> ", c.consumerName)
		return err
	}
	<-c.context.Done()
	fmt.Println("Consumer done!")
	return nil
}

func (c *Consumer) startSubscribers(ctx context.Context, consumerBatchSize int) error {
	// Create JetStream Context
	js, err := c.nc.JetStream(nats.MaxWait(30 * time.Second))

	if err != nil {
		errorCount.WithLabelValues("Get Jetstream Context Error").Inc()
		fmt.Println("Error getting jetstream context :", err, " -> ", c.streamName, " -> ", c.consumerName)
		return err
	}
	fmt.Println("Starting subscriber", " -> ", c.streamName, " -> ", c.consumerName, " -> ", c.filterSubject)
	sub, err := js.PullSubscribe(c.filterSubject, c.consumerName, nats.Bind(c.streamName, c.consumerName), nats.ConsumerMemoryStorage())
	if err != nil {
		fmt.Println("Subscription creation error: ", err, " -> ", c.streamName, " -> ", c.consumerName, " -> ", c.filterSubject)
		errorCount.WithLabelValues("Fetch Error").Inc()
		return err
	}
	defer sub.Drain()
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Fetch will return as soon as any message is available rather than wait until the full batch size is available, using a batch size of more than 1 allows for higher throughput when needed.
		msgs, err := sub.Fetch(consumerBatchSize, nats.Context(ctx))
		if err != nil {
			fmt.Println("Consumer fetch error: ", err, " -> ", c.streamName, " -> ", c.consumerName, " -> ", c.filterSubject)
			errorCount.WithLabelValues("Fetch Error").Inc()
		} else {
			for _, msg := range msgs {
				c.handleMsg(msg)
			}
		}
	}
}

func (c *Consumer) handleMsg(m *nats.Msg) {
	err := m.Ack()
	if err != nil {
		errorCount.WithLabelValues("Message Ack Error").Inc()
		fmt.Printf("RECV: Ack error: %v\n", err)
		return
	}

	msgResult := PerfMsg{}
	err = json.Unmarshal(m.Data, &msgResult)
	if err != nil {
		errorCount.WithLabelValues("Json Unmarshal Error").Inc()
		fmt.Printf("RECV: Msg error: %v\n", err)
		return
	}

	latency := GetMsgLatency(msgResult)
	msgDuration.WithLabelValues().Observe(latency.Seconds())
	msgCount.WithLabelValues().Inc()
}

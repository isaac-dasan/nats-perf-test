package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func perfTest() {
	msgSize, _ := strconv.ParseInt(os.Getenv("messageSize"), 0, 64)
	subjectCount, _ := strconv.ParseInt(os.Getenv("subjectCount"), 0, 64)
	messageCount, _ := strconv.ParseInt(os.Getenv("messageCount"), 0, 64)
	publisherParallelismCount, _ := strconv.ParseInt(os.Getenv("publisherParallelismCount"), 0, 64)
	maxPublisherQueueDepth, _ := strconv.ParseInt(os.Getenv("maxPublisherQueueDepth"), 0, 64)
	publisherTimeOutMs, _ := strconv.ParseInt(os.Getenv("publisherTimeOutMs"), 0, 64)
	replication, _ := strconv.ParseInt(os.Getenv("replication"), 0, 64)
	maxAgeMins, _ := strconv.ParseInt(os.Getenv("maxAgeMins"), 0, 64)
	subjectsPerConsumer, _ := strconv.ParseInt(os.Getenv("subjectsPerConsumer"), 0, 64)
	storageType := os.Getenv("storageType")
	streamNamePrefix := os.Getenv("streamNamePrefix")
	curPodNumber := strings.Split(os.Getenv("MY_POD_NAME"), "-")[2]
	curStreamName := streamNamePrefix + "-" + curPodNumber

	fmt.Printf("Starting perf test with currentStreamName- %s", curStreamName)
	fmt.Println()
	// create mock value
	b := make([]byte, msgSize)
	data := ""
	for int64(len(b)) < msgSize {
		data += "test"
		b = []byte(data)
	}

	c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifiers are not blocked
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	nc, err := nats.Connect("nats://nats")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to nats")

	//Remove Previous Stream if present
	removeStream(nc, curStreamName)
	fmt.Println("Removed Stream!")

	subjects, creatErr := createStream(nc, curStreamName, int(subjectCount), maxPublisherQueueDepth, storageType, int(replication), int(maxAgeMins))
	defer func() {
		removeStream(nc, curStreamName)
		fmt.Println("Removed Stream!")
	}()
	defer cancel()
	// Don't panic yourself return or log an error
	if creatErr != nil {
		fmt.Println("No stream created!", creatErr)
		return
	}

	fmt.Println("Stream Created")

	if len(subjects) == 0 {
		fmt.Println("No subject created!")
		return
	}

	obs := newObserver(maxPublisherQueueDepth)
	listenerCtx, _ := context.WithCancel(ctx)
	go listenForPublishRequest(listenerCtx, obs, publisherParallelismCount)

	metricsCtx, _ := context.WithCancel(ctx)
	go recordQueueDepth(metricsCtx, obs)

	var sleepTime time.Duration
	if publisherTimeOutMs > 0 {
		sleepTime = time.Millisecond * time.Duration(publisherTimeOutMs)
	} else {
		sleepTime = time.Millisecond * time.Duration(500)
	}

	for {
		ticker := time.NewTicker(sleepTime)
		select {
		case <-c:
			fmt.Println("Break the loop")
			return
		case <-ticker.C:
			queueCtx, _ := context.WithCancel(ctx)
			consumerCount := int(subjectCount) / int(subjectsPerConsumer)
			obs.queuePublishRequest(queueCtx, nc, curStreamName, int(subjectCount), int(messageCount), consumerCount, b)
		}
	}
}

func recordQueueDepth(ctx context.Context, obs *Observer) {
	var sleepTime time.Duration = time.Millisecond * time.Duration(1000)

	for {
		ticker := time.NewTicker(sleepTime)
		select {
		case <-ctx.Done():
			fmt.Println("Break the loop")
			return
		case <-ticker.C:
			publishQueueDepth.Add(float64(len(obs.reqChannel)))
		}
	}
}

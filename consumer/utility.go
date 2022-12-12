package main

import (
	"fmt"
	"time"
)

type (
	// PerfMsg is a message used for performance testing
	PerfMsg struct {
		Subject string
		Data    []byte
		Created int64
	}

	// PerfMsgResult is the result of a message
	PerfMsgResult struct {
		Subject string
		Latency time.Duration
	}

	// PerfResult is the result of a performance test
	PerfResult struct {
		MaxLatency time.Duration
		Sent       uint64
		Received   uint64
	}
)

func GetSubjectWildCard(streamName string, consumerNum int) string {
	return fmt.Sprintf("%s.consumer-%d.>", streamName, consumerNum)
}

func GetConsumerName(streamName string, consumerNum int) string {
	return fmt.Sprintf("%s-consumer-%d", streamName, consumerNum)
}

func GetSubjectName(streamName string, consumerNum int, subjectNum int) string {
	return fmt.Sprintf("%s.consumer-%d.subject-%d", streamName, consumerNum, subjectNum)
}

// GetTimeFromMsg returns the time from a message
func GetTimeFromMsg(msg PerfMsg) time.Time {
	return time.Unix(0, msg.Created)
}

// GetMsgLatency returns the latency of a message
func GetMsgLatency(msg PerfMsg) time.Duration {
	return time.Since(GetTimeFromMsg(msg))
}

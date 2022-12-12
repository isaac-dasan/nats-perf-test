package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	go exposeMetrics()

	if err := run(); err != nil {
		panic(err.Error())
	}

	fmt.Println("done!")

	os.Exit(0)
}

func exposeMetrics() {
	// setup metrics endpoints
	rtr := http.NewServeMux()
	rtr.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)
	if err := http.ListenAndServe(":3000", rtr); err != nil && err != http.ErrServerClosed {
		log.Fatalln(err)
	}
}

func run() error {
	osSig := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifiers are not blocked
	signal.Notify(osSig, os.Interrupt, syscall.SIGTERM)
	// defer signal.Stop(osSig) // Close the os signal channel to prevent any leak.

	// Connect to a server
	nc, err := nats.Connect("nats://nats")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	processor := newProcessor(ctx, nc)
	/* This will start consumers in the form of go routines per subject in the background.
	We won't wait for consumers to stop until the program is canceled*/
	processor.start()

	<-osSig // Wait for OS signal to cancel the program.
	fmt.Println("Received OS Interrupt signal and stopped the consumers")
	return nil
}

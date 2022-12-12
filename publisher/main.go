package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	fmt.Println("Main func started")
	go func() {
		fmt.Println("Serving metrics endpoint at port 3000")
		// setup metrics endpoints
		rtr := http.NewServeMux()
		rtr.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)
		if err := http.ListenAndServe(":3000", rtr); err != nil && err != http.ErrServerClosed {
			log.Fatalln(err)
		}
	}()

	perfTest()

	os.Exit(0)
}

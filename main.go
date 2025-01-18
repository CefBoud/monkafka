package main

import (
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/CefBoud/monkafka/broker"
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/types"
)

var config = types.Configuration{
	LogDir:                      filepath.Join(os.TempDir(), "MonKafka"),
	BrokerHost:                  "localhost",
	BrokerPort:                  9092,
	FlushIntervalMs:             5000,
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

func main() {
	// TODO: config from args / env
	broker := broker.NewBroker(config)

	log.SetLogLevel(log.DEBUG)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalChannel
		log.Info("\nReceived signal: %s. Shutting down...\n", sig)
		broker.Shutdown()
		os.Exit(0)
	}()

	broker.Startup()
}

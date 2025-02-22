package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/types"
	"github.com/hashicorp/serf/serf"
	"github.com/spf13/cobra"
)

var config = types.Configuration{
	LogDir:                      os.TempDir(),
	BrokerHost:                  "localhost",
	BrokerPort:                  9092,
	SerfConfig:                  serf.DefaultConfig(),
	FlushIntervalMs:             5000,
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "broker",
		Short: "Kafka broker with Raft protocol support",
		Run: func(cmd *cobra.Command, args []string) {
			broker := protocol.NewBroker(&config)
			log.SetLogLevel(log.DEBUG)

			// Handle termination signals (e.g., Ctrl+C)
			signalChannel := make(chan os.Signal, 1)
			signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				sig := <-signalChannel
				log.Info("Received signal: %s. Shutting down...", sig)
				broker.Shutdown()
				os.Exit(0)
			}()

			// Start the broker
			broker.Startup()
		},
	}
	// Add flags using Cobra
	rootCmd.Flags().BoolVar(&config.Bootstrap, "bootstrap", false, "Indicates if this broker should bootstrap the first broker in the cluster")
	rootCmd.Flags().IntVar(&config.NodeID, "node-id", -1, "Unique node ID for this broker instance")
	rootCmd.MarkFlagRequired("node-id")
	rootCmd.Flags().StringVar(&config.RaftAddress, "raft-addr", "localhost:2221", "The address where Raft will bind")
	rootCmd.Flags().StringVar(&config.SerfAddress, "serf-addr", "127.0.0.1:3331", "The address where Serf will bind")
	rootCmd.Flags().StringVar(&config.SerfJoinAddress, "serf-join", "", "The Serf Join address")
	rootCmd.Flags().IntVar(&config.BrokerPort, "broker-port", 9092, "Port where the broker will listen for client connections")

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		log.Panic("Failed to execute root command %v", err)
	}
}

package test

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CefBoud/monkafka/broker"
	"github.com/CefBoud/monkafka/types"
)

var TestConfig = types.Configuration{
	LogDir:                      filepath.Join(os.TempDir(), "MonKafkaTest"),
	BrokerHost:                  "localhost",
	BrokerPort:                  19092,
	FlushIntervalMs:             5000,
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

var BootstrapServers = fmt.Sprintf("%s:%d", TestConfig.BrokerHost, TestConfig.BrokerPort)
var HomeDir, _ = os.UserHomeDir()
var KafkaBinDir = HomeDir + "/kafka_2.13-3.9.0/bin" // assumes kafka is in HomeDir

func TestMain(m *testing.M) {
	// Initialization logic
	log.Println("Setup: Initializing resources")

	v, exists := os.LookupEnv("KAFKA_BIN_DIR")
	if exists {
		KafkaBinDir = v
	}
	if _, err := os.Stat(KafkaBinDir); err != nil {
		log.Printf("Ensure Kafka bin dir exists. %v", err)
		os.Exit(1)
	}
	broker := broker.NewBroker(TestConfig)
	go broker.Startup()

	// Run the tests
	exitCode := m.Run()

	// Teardown logic
	log.Println("Teardown: Cleaning up resources")
	broker.Shutdown()
	log.Printf("deleting LogDir %v", TestConfig.LogDir)
	os.RemoveAll(TestConfig.LogDir)

	os.Exit(exitCode)
}

func TestTopicCreation(t *testing.T) {
	topicName := "titi"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-topics.sh"),
		"--bootstrap-server", BootstrapServers,
		"--create",
		"--topic", topicName,
	)
	output, err := cmd.CombinedOutput()

	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Created topic %s.", topicName)) {
		t.Errorf("Expected output to contain 'Created topic %s.'", topicName)
	}
}

func TestProducerAndConsumer(t *testing.T) {
	nbRecords := "100" // "100000"
	topicName := "test-topic"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
		"--topic", topicName,
		"--num-records", nbRecords,
		"--record-size", "500",
		"--throughput", "100000",
		"--producer-props", "acks=1", "batch.size=16384", "linger.ms=5", fmt.Sprintf("bootstrap.servers=%s", BootstrapServers),
	)
	stdout, err := cmd.Output()

	if err != nil {
		t.Error(err.Error())
	}

	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", nbRecords,
	)
	output, err := consumerCmd.CombinedOutput()
	log.Println(string(stdout))
	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Processed a total of %s messages", nbRecords)) {
		t.Errorf("Expected consumer output to contain 'Processed a total of %s messages'", nbRecords)
	}
}

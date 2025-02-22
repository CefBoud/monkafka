package cluster

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CefBoud/monkafka/logging"
	broker "github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/types"
	"github.com/hashicorp/serf/serf"
)

var LogDir = filepath.Join(os.TempDir(), "MonKafkaTestCluster")

var TestConfig = types.Configuration{
	LogDir:                      LogDir,
	BrokerHost:                  "localhost",
	BrokerPort:                  19091,
	FlushIntervalMs:             5000,
	NodeID:                      1,
	Bootstrap:                   true,
	RaftAddress:                 "localhost:12221",
	SerfAddress:                 "127.0.0.1:13331",
	SerfConfig:                  serf.DefaultConfig(),
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

var TestConfig2 = types.Configuration{
	LogDir:                      LogDir,
	BrokerHost:                  "localhost",
	BrokerPort:                  19092,
	FlushIntervalMs:             5000,
	NodeID:                      2,
	Bootstrap:                   false,
	RaftAddress:                 "localhost:12222",
	SerfAddress:                 "127.0.0.1:13332",
	SerfJoinAddress:             "127.0.0.1:13331",
	SerfConfig:                  serf.DefaultConfig(),
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

var TestConfig3 = types.Configuration{
	LogDir:                      LogDir,
	BrokerHost:                  "localhost",
	BrokerPort:                  19093,
	FlushIntervalMs:             5000,
	NodeID:                      3,
	Bootstrap:                   false,
	RaftAddress:                 "localhost:12223",
	SerfAddress:                 "127.0.0.1:13333",
	SerfJoinAddress:             "127.0.0.1:13331",
	SerfConfig:                  serf.DefaultConfig(),
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

var BootstrapServers = fmt.Sprintf("%s:%d", TestConfig.BrokerHost, TestConfig.BrokerPort)
var HomeDir, _ = os.UserHomeDir()
var KafkaBinDir = HomeDir + "/kafka_2.13-3.9.0/bin/" // assumes kafka is in HomeDir

var topicName = "cluster-test-topic"

func TestMain(m *testing.M) {
	// Initialization logic
	logging.SetLogLevel(logging.INFO)
	log.Println("Setup: Initializing resources")
	os.RemoveAll(TestConfig.LogDir)
	v, exists := os.LookupEnv("KAFKA_BIN_DIR")
	if exists {
		KafkaBinDir = v
	}
	if _, err := os.Stat(KafkaBinDir); err != nil {
		log.Printf("Ensure Kafka bin dir exists. %v", err)
		os.Exit(1)
	}
	broker1 := broker.NewBroker(&TestConfig)
	go broker1.Startup()
	time.Sleep(5 * time.Second)
	broker2 := broker.NewBroker(&TestConfig2)
	go broker2.Startup()
	broker3 := broker.NewBroker(&TestConfig3)
	go broker3.Startup()
	// wait for the cluster to settle
	time.Sleep(3 * time.Second)
	// Run the tests
	exitCode := m.Run()

	// Teardown
	log.Println("Teardown: Cleaning up resources")
	// broker1.Shutdown()
	// broker2.Shutdown()
	// broker3.Shutdown()

	os.Exit(exitCode)
}

func TestTopicCreation(t *testing.T) {
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-topics.sh"),
		"--bootstrap-server", BootstrapServers,
		"--create",
		"--topic", topicName,
		"--partitions", "10",
	)
	output, err := cmd.CombinedOutput()

	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Created topic %s.", topicName)) {
		t.Errorf("Expected output to contain 'Created topic %s.'. Output: %v", topicName, string(output))
	}

	cmd = exec.Command(
		filepath.Join(KafkaBinDir, "kafka-topics.sh"),
		"--bootstrap-server", BootstrapServers,
		"--describe",
		"--topic", topicName,
	)
	output, err = cmd.CombinedOutput()

	if err != nil {
		logging.Error("Describe failed .. %v", string(output))
		t.Error(err.Error())
	}

	// each node id should lead at least one partition
	nodeIds := []int{TestConfig.NodeID, TestConfig2.NodeID, TestConfig3.NodeID}

	for _, nodeID := range nodeIds {
		expected := fmt.Sprintf("Leader: %d", nodeID)
		if !strings.Contains(string(output), expected) {
			t.Errorf("Expected output to contain '%s'. Output: %v", expected, string(output))
		}
	}

}

func TestProducerAndConsumer(t *testing.T) {
	nbRecords := "1000000"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
		"--topic", topicName,
		"--num-records", nbRecords,
		"--record-size", "500",
		"--throughput", "100000",
		"--producer-props", "acks=1", "batch.size=16384", "linger.ms=5", fmt.Sprintf("bootstrap.servers=%s", BootstrapServers),
	)
	_, err := cmd.Output()

	if err != nil {
		t.Error(err.Error())
	}

	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", nbRecords,
		"--from-beginning",
	)
	output, err := consumerCmd.CombinedOutput()

	if err != nil {
		t.Error(err.Error())
	}
	if !strings.Contains(string(output), fmt.Sprintf("Processed a total of %s messages", nbRecords)) {
		t.Errorf("Expected consumer output to contain 'Processed a total of %s messages'. Output: %v", nbRecords, string(output))
	}
}

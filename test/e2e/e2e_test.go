package test

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/CefBoud/monkafka/compress"
	"github.com/CefBoud/monkafka/logging"
	broker "github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/hashicorp/serf/serf"
)

var TestConfig = types.Configuration{
	LogDir:          filepath.Join(os.TempDir(), "MonKafkaTest"),
	BrokerHost:      "localhost",
	BrokerPort:      19090,
	FlushIntervalMs: 5000,
	NodeID:          1,

	Bootstrap:                   true,
	RaftAddress:                 "localhost:12220",
	SerfAddress:                 "127.0.0.1:13330",
	SerfConfig:                  serf.DefaultConfig(),
	LogRetentionCheckIntervalMs: 1000 * 30,          // 30 sec  //5 * 60 * 1000, // 5 min
	LogRetentionMs:              3 * 60 * 60 * 1000, // 3h //604800000 (7 days)
	LogSegmentSizeBytes:         104857600 * 5,      // 500 MiB
	LogSegmentMs:                1800000,            // 30 min

}

var BootstrapServers = fmt.Sprintf("%s:%d", TestConfig.BrokerHost, TestConfig.BrokerPort)
var HomeDir, _ = os.UserHomeDir()
var KafkaBinDir = HomeDir + "/kafka_2.13-3.9.0/bin/" // assumes kafka is in HomeDir

func TestMain(m *testing.M) {
	// Initialization logic
	logging.SetLogLevel(logging.DEBUG)
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
	broker := broker.NewBroker(&TestConfig)

	go broker.Startup()

	// Run the tests
	exitCode := m.Run()

	// Teardown logic
	log.Println("Teardown: Cleaning up resources")
	broker.Shutdown()
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
		t.Errorf("Expected output to contain 'Created topic %s.'. Output: %v", topicName, string(output))
	}
}

func TestProducerAndConsumer(t *testing.T) {
	nbRecords := "1000" // "100000"
	topicName := "test-topic"
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

func TestOffsetCommitLogFormat(t *testing.T) {
	nbRecords := "100" // "100000"
	topicName := "test-offset-commit-topic"
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
		filepath.Join(KafkaBinDir, "kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", nbRecords,
		"--consumer-property", "enable.auto.commit=true",
		"--from-beginning",
	)
	err = consumerCmd.Run()
	if err != nil {
		t.Error(err.Error())
	}

	// output has a | which confuses exec. We run the command using sh -c to address that
	dumpLogCmd := exec.Command(
		"sh", "-c", fmt.Sprintf(
			"%s --offsets-decoder --files %s",
			filepath.Join(KafkaBinDir, "kafka-dump-log.sh"),
			filepath.Join(TestConfig.LogDir, "__consumer-offsets-0/00000000000000000000.log"),
		),
	)

	output, err := dumpLogCmd.CombinedOutput()
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !strings.Contains(string(output), fmt.Sprintf("\"topic\":\"%v\"", topicName)) ||
		!strings.Contains(string(output), fmt.Sprintf("\"offset\":%v", nbRecords)) {
		t.Errorf("Expected kafka-dump-log output to contain topic %v and offset %v. Output: %v", topicName, nbRecords, string(output))
	}
}

func TestGroupOffsetResume(t *testing.T) {
	nbRecords := "100"

	topicName := "test-group-offset-resume"
	cmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
		"--topic", topicName,
		"--num-records", nbRecords,
		"--payload-monotonic", // payload will be 1,2 ...nbRecords
		"--throughput", "100000",
		"--producer-props", "acks=1", "batch.size=1", fmt.Sprintf("bootstrap.servers=%s", BootstrapServers), // batch.size=1 to have fine-grained consumption
	)
	_, err := cmd.Output()

	if err != nil {
		t.Error(err.Error())
	}

	// we consume nbConsumedRecords from beginning and commit offset
	// next consumed message for the same group id should be "nbConsumedRecords" given we used --payload-monotonic
	// with `kafka-producer-perf-test`
	nbConsumedRecords := "33"
	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", nbConsumedRecords,
		"--consumer-property", "enable.auto.commit=true",
		"--consumer-property", "group.id=test1",
		"--from-beginning",
	)
	err = consumerCmd.Run()
	if err != nil {
		t.Error(err.Error())
	}

	// consumer resumes from last offset at `nbConsumedRecords`
	consumerCmd = exec.Command(
		filepath.Join(KafkaBinDir, "kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--max-messages", "1",
		"--consumer-property", "group.id=test1",
	)
	output, err := consumerCmd.CombinedOutput()
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !strings.Contains(string(output), nbConsumedRecords) {
		t.Errorf("Expected kafka-console-consumer output to contain value '%v'. Output: %v", nbConsumedRecords, string(output))
	}
}

func TestDecompression(t *testing.T) {
	nbRecords := 10

	var compressions = map[string]compress.CompressionType{
		"gzip":   compress.GZIP,
		"snappy": compress.SNAPPY,
		"lz4":    compress.LZ4,
		"zstd":   compress.ZSTD,
	}

	for compressionName, compressionTypeBits := range compressions {
		topicName := "test-decompression-" + compressionName
		cmd := exec.Command(
			filepath.Join(KafkaBinDir, "kafka-producer-perf-test.sh"),
			"--topic", topicName,
			"--num-records", strconv.Itoa(nbRecords),
			"--payload-monotonic", // payload will be 1,2 ...nbRecords
			"--throughput", "100000",
			"--producer-props",
			"compression.type="+compressionName,
			"linger.ms=1000", "batch.size=1000", // make sure all records are in the same batch
			"bootstrap.servers="+BootstrapServers)

		o, err := cmd.Output()

		fmt.Println(string(o))
		if err != nil {
			t.Error(err.Error())
		}

		recordBytes, _ := storage.GetRecordBatch(0, topicName, 0)
		rb := storage.ReadRecordBatch(recordBytes)

		compressionType := compress.CompressionType(rb.Attributes & 0x07)
		if compressionType != compressionTypeBits {
			t.Errorf("Expected %v compression in '%+v'. but found compression code : %v", compressionName, rb, compressionType)
		}

		// first Record batch should contain all records given linger.ms and batch.size values
		records := storage.ReadRecords(rb)

		// `nbRecords-1`` because both start at offset 0
		if string(records[nbRecords-1].Value) != strconv.Itoa(nbRecords-1) {
			t.Errorf("Expected last record's value in record batch to be '%v'. but found: '%v'", strconv.Itoa(nbRecords-1), string(records[nbRecords-1].Value))
		}
	}
}

func TestConsumeFromUnknownTopic(t *testing.T) {
	topicName := "fugazi-topic"
	consumerCmd := exec.Command(
		filepath.Join(KafkaBinDir, "/kafka-console-consumer.sh"),
		"--bootstrap-server", BootstrapServers,
		"--topic", topicName,
		"--timeout-ms", "1000",
		"--consumer-property", "allow.auto.create.topics=false",
	)
	output, err := consumerCmd.CombinedOutput()

	if !strings.Contains(string(output), fmt.Sprintf("{%s=UNKNOWN_TOPIC_OR_PARTITION}", topicName)) {
		t.Errorf("Expected output to contain '{%s=UNKNOWN_TOPIC_OR_PARTITION}'. Output: %v. Err: %v", topicName, string(output), err)
	}
}

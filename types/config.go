package types

type Configuration struct {
	LogDir                      string
	BrokerHost                  string
	BrokerPort                  uint32
	FlushIntervalMs             uint64 // flush.ms:time in ms that a message in any topic is kept in memory before flushed to disk.
	LogRetentionCheckIntervalMs uint64 // check interval to manage log retention
	LogRetentionMs              uint64 // The number of milliseconds to keep a log file before deleting it
	LogSegmentSizeBytes         uint64 // segment.bytes: controls the segment file size for the log
	LogSegmentMs                uint64 // segment.ms: period of time after which Kafka will force the log to roll
}

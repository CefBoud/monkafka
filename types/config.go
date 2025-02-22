package types

import "github.com/hashicorp/serf/serf"

// Configuration represents the broker configuration and settings
type Configuration struct {
	Bootstrap                   bool // Bootstrap is used to bring up the first broker. It is required so that it can elect a leader without any other nodes
	NodeID                      int
	RaftID                      string
	RaftAddress                 string
	SerfAddress                 string
	SerfJoinAddress             string
	SerfConfig                  *serf.Config
	LogDir                      string
	BrokerHost                  string `kafka:"CompactString"`
	BrokerPort                  int
	FlushIntervalMs             uint64 // flush.ms:time in ms that a message in any topic is kept in memory before flushed to disk.
	LogRetentionCheckIntervalMs uint64 // check interval to manage log retention
	LogRetentionMs              uint64 // The number of milliseconds to keep a log file before deleting it
	LogSegmentSizeBytes         uint64 // segment.bytes: controls the segment file size for the log
	LogSegmentMs                uint64 // segment.ms: period of time after which Kafka will force the log to roll
}

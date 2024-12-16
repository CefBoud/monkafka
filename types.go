package main

// Requests
// type RecordBatch struct {
// 	BaseOffset           int64
// 	BatchLength          int32
// 	PartitionLeaderEpoch int32
// 	Magic                int8
// 	CRC                  uint32
// 	Attributes           int16
// 	LastOffsetDelta      int32 // delta added to BaseOffset to get the Batch's last offset
// 	BaseTimestamp        int64
// 	MaxTimestamp         int64
// 	ProducerId           int64
// 	ProducerEpoch        int16
// 	BaseSequence         int32
// 	records              []Record
// }

// type Record struct {
// 	Length         int // varint (usually int32 or int64 depending on the actual size)
// 	Attributes     int8
// 	TimestampDelta int64 // varlong: delta added the batch's BaseTimestamp
// 	OffsetDelta    int   // varint: delta added to the batch's BaseOffset
// 	KeyLength      int   // varint
// 	Key            []byte
// 	ValueLen       int // varint
// 	Value          []byte
// 	Headers        []Header
// }
// type Header struct {
// 	HeaderKeyLength   int
// 	HeaderKey         string
// 	HeaderValueLength int
// 	Value             []byte
// }

type TopicData struct {
	name           string
	partition_data []PartitionData
}
type PartitionData struct {
	index       uint32
	recordsData []byte
}

// Responses
type APIKey struct {
	apiKey     uint16
	minVersion uint16
	maxVersion uint16
}

type APIVersionsResponse struct {
	errorCode uint16
	apiKeys   []APIKey
}

type broker struct {
	node_id uint32
	host    string
	port    uint32
	rack    string //nullable: if it is empty, we set length to -1
}
type partition struct {
	error_code       uint16
	partition_index  uint32
	leader_id        uint32
	leader_epoch     uint32
	replica_nodes    []uint32
	isr_nodes        []uint32
	offline_replicas []uint32
}
type topic struct {
	error_code                  int16
	name                        string
	topic_id                    [16]byte
	is_internal                 bool
	partitions                  []partition
	topic_authorized_operations uint32
}
type metadataResponse struct {
	throttle_time_ms int32
	brokers          []broker
	cluster_id       string //nullable
	controller_id    int32
	topics           []topic
}

type CreateTopicsResponse struct {
	ThrottleTimeMs uint32
	Topics         []Topic
}

type Topic struct {
	Name              string
	TopicID           [16]byte
	ErrorCode         uint16
	ErrorMessage      string
	NumPartitions     uint32
	ReplicationFactor uint16
	Configs           []Config
}

type Config struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

type InitProducerId struct {
	throttle_time_ms uint32
	error_code       uint16
	producer_id      uint64
	producer_epoch   uint16
}

// produce
type ProduceResponse struct {
	ProduceTopicResponses []ProduceTopicResponse
	ThrottleTimeMs        uint32
}
type ProduceTopicResponse struct {
	Name                      string
	ProducePartitionResponses []ProducePartitionResponse
}
type ProducePartitionResponse struct {
	Index           uint32
	ErrorCode       uint16
	BaseOffset      uint64
	LogAppendTimeMs uint64
	LogStartOffset  uint64
	RecordErrors    []RecordError
	ErrorMessage    string // compact_nullable
}

type RecordError struct {
	BatchIndex             uint32
	BatchIndexErrorMessage string // compact_nullable
}

// FindCoordinator
type Coordinator struct {
	Key          string
	NodeID       uint32
	Host         string
	Port         uint32
	ErrorCode    uint16
	ErrorMessage string
}

// JoinGroup

type Member struct {
	MemberID        string
	GroupInstanceID string
	Metadata        []byte
}

type JoinGroupResponse struct {
	ThrottleTimeMS uint32
	ErrorCode      uint16
	GenerationID   uint32
	ProtocolType   string
	ProtocolName   string
	Leader         string
	SkipAssignment bool
	MemberID       string
	Members        []Member
}

// Offset Fetch
type OffsetFetchResponse struct {
	ThrottleTimeMs uint32
	Groups         []OffsetFetchGroup
}

type OffsetFetchGroup struct {
	GroupID   string
	Topics    []OffsetFetchTopic
	ErrorCode uint16
}

type OffsetFetchTopic struct {
	Name       string
	Partitions []OffsetFetchPartition
	ErrorCode  uint16
}

type OffsetFetchPartition struct {
	PartitionIndex       uint32
	CommittedOffset      uint64
	CommittedLeaderEpoch uint32
	Metadata             string
	ErrorCode            uint16
}

// Fetch
type FetchResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionId      uint32
	Responses      []FetchTopicResponse
}

type FetchTopicResponse struct {
	TopicName  string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex       uint32
	ErrorCode            uint16
	HighWatermark        uint64
	LastStableOffset     uint64
	LogStartOffset       uint64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica uint32
	Records              []byte
}

type AbortedTransaction struct {
	ProducerId  uint64
	FirstOffset uint64
}

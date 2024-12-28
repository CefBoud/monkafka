package protocol

// Responses
type APIKey struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

type APIVersionsResponse struct {
	ErrorCode uint16
	ApiKeys   []APIKey
}

type MetadataResponseBroker struct {
	Node_id uint32
	Host    string
	Port    uint32
	Rack    string //nullable: if it is empty, we set length to -1
}
type MetadataResponsePartition struct {
	Error_code       uint16
	Partition_index  uint32
	Leader_id        uint32
	Leader_epoch     uint32
	Replica_nodes    []uint32
	Isr_nodes        []uint32
	Offline_replicas []uint32
}
type MetadataResponseTopic struct {
	Error_code                  int16
	Name                        string
	Topic_id                    [16]byte
	Is_internal                 bool
	Partitions                  []MetadataResponsePartition
	Topic_authorized_operations uint32
}
type MetadataResponse struct {
	Throttle_time_ms int32
	Brokers          []MetadataResponseBroker
	Cluster_id       string //nullable
	Controller_id    int32
	Topics           []MetadataResponseTopic
}

type CreateTopicsResponse struct {
	ThrottleTimeMs uint32
	Topics         []CreateTopicsResponseTopic
}

type CreateTopicsResponseTopic struct {
	Name              string
	TopicID           [16]byte
	ErrorCode         uint16
	ErrorMessage      string
	NumPartitions     uint32
	ReplicationFactor uint16
	Configs           []CreateTopicsResponseConfig
}

type CreateTopicsResponseConfig struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

type InitProducerIdResponse struct {
	Throttle_time_ms uint32
	Error_code       uint16
	Producer_id      uint64
	Producer_epoch   uint16
}

// produce
type ProduceResponseTopicData struct {
	Name           string
	Partition_data []ProduceResponsePartitionData
}
type ProduceResponsePartitionData struct {
	Index       uint32
	RecordsData []byte
}
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
type FindCoordinatorResponseCoordinator struct {
	Key          string
	NodeID       uint32
	Host         string
	Port         uint32
	ErrorCode    uint16
	ErrorMessage string
}

// JoinGroup

type JoinGroupResponseMember struct {
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
	Members        []JoinGroupResponseMember
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

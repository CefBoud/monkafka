package protocol

// APIKey represents an API key and its supported version range.
type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

// APIVersionsResponse represents the response for API versions request.
type APIVersionsResponse struct {
	ErrorCode    uint16
	APIKeys      []APIKey
	ThrottleTime uint32
}

// MetadataResponseBroker represents a broker in a metadata response.
type MetadataResponseBroker struct {
	NodeID uint32
	Host   string `kafka:"CompactString"`
	Port   uint32
	Rack   string `kafka:"CompactString"`
}

// MetadataResponsePartition represents partition information in a metadata response.
type MetadataResponsePartition struct {
	ErrorCode       uint16
	PartitionIndex  uint32
	LeaderID        uint32
	LeaderEpoch     uint32
	ReplicaNodes    []uint32
	IsrNodes        []uint32
	OfflineReplicas []uint32
}

// MetadataResponseTopic represents a topic in the metadata response.
type MetadataResponseTopic struct {
	ErrorCode                 uint16
	Name                      string `kafka:"CompactString"`
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                []MetadataResponsePartition
	TopicAuthorizedOperations uint32
}

// MetadataResponse represents a metadata response with brokers, topics, and more.
type MetadataResponse struct {
	ThrottleTimeMs uint32
	Brokers        []MetadataResponseBroker
	ClusterID      string `kafka:"CompactString"` // nullable
	ControllerID   uint32
	Topics         []MetadataResponseTopic
}

// CreateTopicsResponse represents the response to a topic creation request.
type CreateTopicsResponse struct {
	ThrottleTimeMs uint32
	Topics         []CreateTopicsResponseTopic
}

// CreateTopicsResponseTopic represents a topic's creation result.
type CreateTopicsResponseTopic struct {
	Name              string `kafka:"CompactString"`
	TopicID           [16]byte
	ErrorCode         uint16
	ErrorMessage      string `kafka:"CompactString"`
	NumPartitions     uint32
	ReplicationFactor uint16
	Configs           []CreateTopicsResponseConfig
}

// CreateTopicsResponseConfig represents a configuration for a topic.
type CreateTopicsResponseConfig struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

// InitProducerIDResponse represents the response to a producer ID initialization request.
type InitProducerIDResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	ProducerID     uint64
	ProducerEpoch  uint16
}

// ProduceResponseTopicData represents the data for a topic in a produce response.
type ProduceResponseTopicData struct {
	Name          string `kafka:"CompactString"`
	PartitionData []ProduceResponsePartitionData
}

// ProduceResponsePartitionData represents partition-level data for a produce response.
type ProduceResponsePartitionData struct {
	Index       uint32
	RecordsData []byte
}

// ProduceResponse represents the response to a produce request.
type ProduceResponse struct {
	ProduceTopicResponses []ProduceTopicResponse
	ThrottleTimeMs        uint32
}

// ProduceTopicResponse represents the response for a topic in a produce request.
type ProduceTopicResponse struct {
	Name                      string `kafka:"CompactString"`
	ProducePartitionResponses []ProducePartitionResponse
}

// ProducePartitionResponse represents the response for a partition in a produce request.
type ProducePartitionResponse struct {
	Index           uint32
	ErrorCode       uint16
	BaseOffset      uint64
	LogAppendTimeMs uint64
	LogStartOffset  uint64
	RecordErrors    []RecordError
	ErrorMessage    string `kafka:"CompactString"`
}

// RecordError represents an error in a specific batch of records.
type RecordError struct {
	BatchIndex             uint32
	BatchIndexErrorMessage string // compact_nullable
}

// FindCoordinatorResponse represents the response for a coordinator finding request.
type FindCoordinatorResponse struct {
	ThrottleTimeMs uint32
	Coordinators   []FindCoordinatorResponseCoordinator
}

// FindCoordinatorResponseCoordinator represents  a coordinator.
type FindCoordinatorResponseCoordinator struct {
	Key          string `kafka:"CompactString"`
	NodeID       uint32
	Host         string `kafka:"CompactString"`
	Port         uint32
	ErrorCode    uint16
	ErrorMessage string `kafka:"CompactString"`
}

// SyncGroupResponse represents a SyncGroup
type SyncGroupResponse struct {
	ThrottleTimeMs  uint32
	ErrorCode       uint16
	ProtocolType    string `kafka:"CompactString"`
	ProtocolName    string `kafka:"CompactString"`
	AssignmentBytes []byte
}

// HeartbeatResponse represents a HearBeat
type HeartbeatResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
}

// JoinGroupResponseMember represents a member in a join group response.
type JoinGroupResponseMember struct {
	MemberID        string `kafka:"CompactString"`
	GroupInstanceID string `kafka:"CompactString"`
	Metadata        []byte
}

// JoinGroupResponse represents the response to a join group request.
type JoinGroupResponse struct {
	ThrottleTimeMS uint32
	ErrorCode      uint16
	GenerationID   uint32
	ProtocolType   string `kafka:"CompactString"`
	ProtocolName   string `kafka:"CompactString"`
	Leader         string `kafka:"CompactString"`
	SkipAssignment bool
	MemberID       string `kafka:"CompactString"`
	Members        []JoinGroupResponseMember
}

// OffsetFetchResponse represents the response to an offset fetch request.
type OffsetFetchResponse struct {
	ThrottleTimeMs uint32
	Groups         []OffsetFetchGroup
}

// OffsetFetchGroup represents a group in an offset fetch response.
type OffsetFetchGroup struct {
	GroupID   string `kafka:"CompactString"`
	Topics    []OffsetFetchTopic
	ErrorCode uint16
}

// OffsetFetchTopic represents a topic in an offset fetch response.
type OffsetFetchTopic struct {
	Name       string
	Partitions []OffsetFetchPartition
	ErrorCode  uint16
}

// OffsetFetchPartition represents a partition in an offset fetch response.
type OffsetFetchPartition struct {
	PartitionIndex       uint32
	CommittedOffset      uint64
	CommittedLeaderEpoch uint32
	Metadata             string
	ErrorCode            uint16
}

// FetchResponse represents the response to a fetch request.
type FetchResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionID      uint32
	Responses      []FetchTopicResponse
}

// FetchTopicResponse represents the response for a topic in a fetch request.
type FetchTopicResponse struct {
	TopicName  string `kafka:"CompactString"`
	Partitions []FetchPartitionResponse
}

// FetchPartitionResponse represents the response for a partition in a fetch request.
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

// AbortedTransaction represents an aborted transaction in the fetch response.
type AbortedTransaction struct {
	ProducerID  uint64
	FirstOffset uint64
}

// ListOffsetsRequest represents a request to list offsets for specific partitions.
type ListOffsetsRequest struct {
	ReplicaID      uint32
	IsolationLevel uint8
	Topics         []ListOffsetsRequestTopic
}

// ListOffsetsRequestTopic represents a topic in a list offsets request.
type ListOffsetsRequestTopic struct {
	Name       string
	Partitions []ListOffsetsRequestPartition
}

// ListOffsetsRequestPartition represents a partition in a list offsets request.
type ListOffsetsRequestPartition struct {
	PartitionIndex     uint32
	CurrentLeaderEpoch uint32
	Timestamp          uint64
}

// Constants for list offsets timestamps.
var (
	ListOffsetsEarliestTimestamp = -2
	ListOffsetsLatestTimestamp   = -1
	ListOffsetsMaxTimestamp      = -3
)

// ListOffsetsResponse represents the response to a list offsets request.
type ListOffsetsResponse struct {
	ThrottleTimeMs uint32
	Topics         []ListOffsetsResponseTopic
}

// ListOffsetsResponseTopic represents a topic in a list offsets response.
type ListOffsetsResponseTopic struct {
	Name       string `kafka:"CompactString"`
	Partitions []ListOffsetsResponsePartition
}

// ListOffsetsResponsePartition represents a partition in a list offsets response.
type ListOffsetsResponsePartition struct {
	PartitionIndex uint32
	ErrorCode      uint16
	Timestamp      uint64
	Offset         uint64
	LeaderEpoch    uint32
}

// OffsetCommitRequest represents a request to commit offsets.
type OffsetCommitRequest struct {
	GroupID                   string
	GenerationIDOrMemberEpoch uint32
	MemberID                  string
	GroupInstanceID           string
	Topics                    []OffsetCommitRequestTopic
}

// OffsetCommitRequestTopic represents a topic in an offset commit request.
type OffsetCommitRequestTopic struct {
	Name       string
	Partitions []OffsetCommitRequestPartition
}

// OffsetCommitRequestPartition represents a partition in an offset commit request.
type OffsetCommitRequestPartition struct {
	PartitionIndex       uint32
	CommittedOffset      uint64
	CommittedLeaderEpoch uint32
	CommittedMetadata    string `kafka:"CompactString"`
}

// OffsetCommitResponse represents the response to an offset commit request.
type OffsetCommitResponse struct {
	ThrottleTimeMs uint32
	Topics         []OffsetCommitResponseTopic
}

// OffsetCommitResponseTopic represents a topic in an offset commit response.
type OffsetCommitResponseTopic struct {
	Name       string `kafka:"CompactString"`
	Partitions []OffsetCommitResponsePartition
}

// OffsetCommitResponsePartition represents a partition in an offset commit response.
type OffsetCommitResponsePartition struct {
	PartitionIndex uint32
	ErrorCode      uint16
}

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

// MetadataRequest represents a metadata request.
type MetadataRequest struct {
	Topics                           []MetadataRequestTopic
	AllowAutoTopicCreation           bool
	IncludeTopicAuthorizedOperations bool
}

// MetadataRequestTopic represents a topic in the metadata request.
type MetadataRequestTopic struct {
	TopicID [16]byte
	Name    string `kafka:"CompactString"`
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

// CreateTopicsRequest represents the Kafka request to create topics.
type CreateTopicsRequest struct {
	Topics       []CreateTopicsRequestTopic
	TimeoutMs    uint32
	ValidateOnly bool
}

// CreateTopicsRequestTopic represents the details of a topic to be created.
type CreateTopicsRequestTopic struct {
	Name              string `kafka:"CompactString"`
	NumPartitions     uint32
	ReplicationFactor uint16
	Assignments       []CreateTopicsRequestAssignment
	Configs           []CreateTopicsRequestConfig
}

// CreateTopicsRequestAssignment represents the partition assignments for a topic.
type CreateTopicsRequestAssignment struct {
	PartitionIndex uint32
	BrokerIds      []uint32
}

// CreateTopicsRequestConfig represents the configuration for a topic.
type CreateTopicsRequestConfig struct {
	Name  string `kafka:"CompactString"`
	Value string `kafka:"CompactNullableString"`
}

// CreateTopicsResponseConfig represents a configuration for a topic.
type CreateTopicsResponseConfig struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

// InitProducerIDRequest represents the request for producer ID initialization.
type InitProducerIDRequest struct {
	TransactionalID      string `kafka:"CompactString"`
	TransactionTimeoutMs uint32
	ProducerID           uint64
	ProducerEpoch        uint16
}

// InitProducerIDResponse represents the response to a producer ID initialization request.
type InitProducerIDResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	ProducerID     uint64
	ProducerEpoch  uint16
}

// ProduceRequest represents the details of a ProduceRequest.
type ProduceRequest struct {
	TransactionalID string `kafka:"CompactNullableString"`
	Acks            uint16
	TimeoutMs       uint32
	TopicData       []ProduceRequestTopicData
}

// ProduceRequestTopicData represents the topic data in a ProduceRequest.
type ProduceRequestTopicData struct {
	Name          string `kafka:"CompactString"`
	PartitionData []ProduceRequestPartitionData
}

// ProduceRequestPartitionData represents the partition data in a ProduceRequest.
type ProduceRequestPartitionData struct {
	Index   uint32
	Records []byte
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

// SyncGroupRequest represents the details of a SyncGroupRequest.
type SyncGroupRequest struct {
	GroupID         string `kafka:"CompactString"`
	GenerationID    uint32
	MemberID        string `kafka:"CompactString"`
	GroupInstanceID string `kafka:"CompactNullableString"`
	ProtocolType    string `kafka:"CompactNullableString"`
	ProtocolName    string `kafka:"CompactNullableString"`
	Assignments     []SyncGroupRequestMember
}

// SyncGroupRequestMember represents a member and its assignment in the SyncGroupRequest.
type SyncGroupRequestMember struct {
	MemberID   string `kafka:"CompactString"`
	Assignment []byte
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

// JoinGroupRequest represents a JoinGroup request
type JoinGroupRequest struct {
	GroupID            string `kafka:"CompactString"`
	SessionTimeoutMs   uint32
	RebalanceTimeoutMs uint32
	MemberID           string `kafka:"CompactString"`
	GroupInstanceID    string `kafka:"CompactNullableString"` // Nullable fields are represented as empty strings if not set
	ProtocolType       string `kafka:"CompactString"`
	Protocols          []JoinGroupRequestProtocol
	Reason             string `kafka:"CompactNullableString"` // Nullable fields are represented as empty strings if not set
}

// JoinGroupRequestProtocol represents a protocol in JoinGroupRequest
type JoinGroupRequestProtocol struct {
	Name     string `kafka:"CompactString"`
	Metadata []byte
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

// OffsetFetchRequest represents the offset fetch request.
type OffsetFetchRequest struct {
	Groups         []OffsetFetchRequestGroup
	RequiresStable bool
}

// OffsetFetchRequestGroup represents a group in OffsetFetchRequest
type OffsetFetchRequestGroup struct {
	GroupID     string `kafka:"CompactString"`
	MemberID    string `kafka:"CompactNullableString"`
	MemberEpoch uint32
	Topics      []OffsetFetchRequestTopic
}

// OffsetFetchRequestTopic represents a topic in OffsetFetchRequestGroup
type OffsetFetchRequestTopic struct {
	Name             string `kafka:"CompactString"`
	PartitionIndexes []uint32
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

// FetchRequest represents the details of a FetchRequest (Version: 12).
type FetchRequest struct {
	ReplicaID           uint32
	MaxWaitMs           uint32
	MinBytes            uint32
	MaxBytes            uint32
	IsolationLevel      uint8
	SessionID           uint32
	SessionEpoch        uint32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopic
	RackID              string `kafka:"CompactString"`
}

// FetchRequestTopic represents the topic-level data in a FetchRequest.
type FetchRequestTopic struct {
	Name       string `kafka:"CompactString"`
	Partitions []FetchRequestPartitionData
}

// FetchRequestPartitionData represents the partition-level data in a FetchRequest.
type FetchRequestPartitionData struct {
	PartitionIndex     uint32
	CurrentLeaderEpoch uint32
	FetchOffset        uint64
	LastFetchedEpoch   uint32
	LogStartOffset     uint64
	PartitionMaxBytes  uint32
}

// FetchRequestForgottenTopic represents the forgotten topic data in a FetchRequest.
type FetchRequestForgottenTopic struct {
	Topic      string `kafka:"CompactString"`
	Partitions []uint32
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

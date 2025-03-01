package protocol

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

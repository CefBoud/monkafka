package protocol

// https://kafka.apache.org/protocol#protocol_error_codes

// Error is a struct to hold the code, message, and retriability status
type Error struct {
	Code        int16
	Message     string
	IsRetriable bool
}

// Define each error as a variable of type Error
var (
	ErrUnknownServerError           = Error{Code: -1, Message: "The server experienced an unexpected error when processing the request.", IsRetriable: false}
	ErrNone                         = Error{Code: 0, Message: "", IsRetriable: false}
	ErrOffsetOutOfRange             = Error{Code: 1, Message: "The requested offset is not within the range of offsets maintained by the server.", IsRetriable: false}
	ErrCorruptMessage               = Error{Code: 2, Message: "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.", IsRetriable: true}
	ErrUnknownTopicOrPartition      = Error{Code: 3, Message: "This server does not host this topic-partition.", IsRetriable: true}
	ErrInvalidFetchSize             = Error{Code: 4, Message: "The requested fetch size is invalid.", IsRetriable: false}
	ErrLeaderNotAvailable           = Error{Code: 5, Message: "There is no leader for this topic-partition as we are in the middle of a leadership election.", IsRetriable: true}
	ErrNotLeaderOrFollower          = Error{Code: 6, Message: "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.", IsRetriable: true}
	ErrRequestTimedOut              = Error{Code: 7, Message: "The request timed out.", IsRetriable: true}
	ErrBrokerNotAvailable           = Error{Code: 8, Message: "The broker is not available.", IsRetriable: false}
	ErrReplicaNotAvailable          = Error{Code: 9, Message: "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.", IsRetriable: true}
	ErrMessageTooLarge              = Error{Code: 10, Message: "The request included a message larger than the max message size the server will accept.", IsRetriable: false}
	ErrStaleControllerEpoch         = Error{Code: 11, Message: "The controller moved to another broker.", IsRetriable: false}
	ErrOffsetMetadataTooLarge       = Error{Code: 12, Message: "The metadata field of the offset request was too large.", IsRetriable: false}
	ErrNetworkException             = Error{Code: 13, Message: "The server disconnected before a response was received.", IsRetriable: true}
	ErrCoordinatorLoadInProgress    = Error{Code: 14, Message: "The coordinator is loading and hence can't process requests.", IsRetriable: true}
	ErrCoordinatorNotAvailable      = Error{Code: 15, Message: "The coordinator is not available.", IsRetriable: true}
	ErrNotCoordinator               = Error{Code: 16, Message: "This is not the correct coordinator.", IsRetriable: true}
	ErrInvalidTopicException        = Error{Code: 17, Message: "The request attempted to perform an operation on an invalid topic.", IsRetriable: false}
	ErrRecordListTooLarge           = Error{Code: 18, Message: "The request included message batch larger than the configured segment size on the server.", IsRetriable: false}
	ErrNotEnoughReplicas            = Error{Code: 19, Message: "Messages are rejected since there are fewer in-sync replicas than required.", IsRetriable: true}
	ErrNotEnoughReplicasAfterAppend = Error{Code: 20, Message: "Messages are written to the log, but to fewer in-sync replicas than required.", IsRetriable: true}
	ErrInvalidRequiredAcks          = Error{Code: 21, Message: "Produce request specified an invalid value for required acks.", IsRetriable: false}
	ErrIllegalGeneration            = Error{Code: 22, Message: "Specified group generation id is not valid.", IsRetriable: false}
	ErrInconsistentGroupProtocol    = Error{Code: 23, Message: "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.", IsRetriable: false}
	ErrInvalidGroupID               = Error{Code: 24, Message: "The configured groupId is invalid.", IsRetriable: false}
	ErrUnknownMemberID              = Error{Code: 25, Message: "The coordinator is not aware of this member.", IsRetriable: false}
	ErrInvalidSessionTimeout        = Error{Code: 26, Message: "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).", IsRetriable: false}
	ErrRebalanceInProgress          = Error{Code: 27, Message: "The group is rebalancing, so a rejoin is needed.", IsRetriable: false}
	ErrInvalidCommitOffsetSize      = Error{Code: 28, Message: "The committing offset data size is not valid.", IsRetriable: false}
	ErrTopicAuthorizationFailed     = Error{Code: 29, Message: "Topic authorization failed.", IsRetriable: false}
	ErrGroupAuthorizationFailed     = Error{Code: 30, Message: "Group authorization failed.", IsRetriable: false}
	ErrClusterAuthorizationFailed   = Error{Code: 31, Message: "Cluster authorization failed.", IsRetriable: false}
	ErrInvalidTimestamp             = Error{Code: 32, Message: "The timestamp of the message is out of acceptable range.", IsRetriable: false}
	ErrUnsupportedSaslMechanism     = Error{Code: 33, Message: "The broker does not support the requested SASL mechanism.", IsRetriable: false}
	ErrIllegalSaslState             = Error{Code: 34, Message: "Request is not valid given the current SASL state.", IsRetriable: false}
	ErrUnsupportedVersion           = Error{Code: 35, Message: "The version of API is not supported.", IsRetriable: false}
	ErrTopicAlreadyExists           = Error{Code: 36, Message: "Topic with this name already exists.", IsRetriable: false}
	ErrInvalidPartitions            = Error{Code: 37, Message: "Number of partitions is below 1.", IsRetriable: false}
	ErrInvalidReplicationFactor     = Error{Code: 38, Message: "Replication factor is below 1 or larger than the number of available brokers.", IsRetriable: false}
	ErrInvalidReplicaAssignment     = Error{Code: 39, Message: "Replica assignment is invalid.", IsRetriable: false}
	ErrInvalidConfig                = Error{Code: 40, Message: "Configuration is invalid.", IsRetriable: false}
)

// ErrorMap associates error codes with corresponding Error structs
var ErrorMap = map[int16]Error{
	-1: ErrUnknownServerError,
	0:  ErrNone,
	1:  ErrOffsetOutOfRange,
	2:  ErrCorruptMessage,
	3:  ErrUnknownTopicOrPartition,
	4:  ErrInvalidFetchSize,
	5:  ErrLeaderNotAvailable,
	6:  ErrNotLeaderOrFollower,
	7:  ErrRequestTimedOut,
	8:  ErrBrokerNotAvailable,
	9:  ErrReplicaNotAvailable,
	10: ErrMessageTooLarge,
	11: ErrStaleControllerEpoch,
	12: ErrOffsetMetadataTooLarge,
	13: ErrNetworkException,
	14: ErrCoordinatorLoadInProgress,
	15: ErrCoordinatorNotAvailable,
	16: ErrNotCoordinator,
	17: ErrInvalidTopicException,
	18: ErrRecordListTooLarge,
	19: ErrNotEnoughReplicas,
	20: ErrNotEnoughReplicasAfterAppend,
	21: ErrInvalidRequiredAcks,
	22: ErrIllegalGeneration,
	23: ErrInconsistentGroupProtocol,
	24: ErrInvalidGroupID,
	25: ErrUnknownMemberID,
	26: ErrInvalidSessionTimeout,
	27: ErrRebalanceInProgress,
	28: ErrInvalidCommitOffsetSize,
	29: ErrTopicAuthorizationFailed,
	30: ErrGroupAuthorizationFailed,
	31: ErrClusterAuthorizationFailed,
	32: ErrInvalidTimestamp,
	33: ErrUnsupportedSaslMechanism,
	34: ErrIllegalSaslState,
	35: ErrUnsupportedVersion,
	36: ErrTopicAlreadyExists,
	37: ErrInvalidPartitions,
	38: ErrInvalidReplicationFactor,
	39: ErrInvalidReplicaAssignment,
	40: ErrInvalidConfig,
}

package types

// GroupID represents the identifier of a consumer group.
type GroupID string

// GroupMetadataTopicPartitionKey represents the key for partition data in group metadata.
type GroupMetadataTopicPartitionKey struct {
	TopicName      string
	PartitionIndex uint32
}

// GroupMetadataTopicPartitionValue represents the value for partition data in group metadata.
type GroupMetadataTopicPartitionValue struct {
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	Metadata             string
}

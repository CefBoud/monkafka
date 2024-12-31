package types

type GroupID string

type GroupMetadataTopicPartitionKey struct {
	TopicName      string
	PartitionIndex uint32
}
type GroupMetadataTopicPartitionValue struct {
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	Metadata             string
}

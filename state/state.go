package state

import (
	"github.com/CefBoud/monkafka/types"
)

// Config holds the global configuration.
var Config *types.Configuration

// TopicStateInstance holds the state of topics.
var TopicStateInstance types.TopicsState = make(types.TopicsState)

// GroupMetadata stores metadata for consumer groups.
var GroupMetadata = make(map[types.GroupID]map[types.GroupMetadataTopicPartitionKey]types.GroupMetadataTopicPartitionValue)

// GetPartition returns a pointer to a partition for a specific topic and partition index.
func GetPartition(topic string, partition uint32) *types.Partition {
	return TopicStateInstance[types.TopicName(topic)][types.PartitionIndex(partition)]
}

// GetPartitions returns all partitions for a specific topic.
func GetPartitions(topic string) []*types.Partition {
	var partitions []*types.Partition
	for _, p := range TopicStateInstance[types.TopicName(topic)] {
		partitions = append(partitions, p)
	}
	return partitions
}

// GroupExists checks if a consumer group exists.
func GroupExists(groupID string) bool {
	_, exists := GroupMetadata[types.GroupID(groupID)]
	return exists
}

// TopicExists checks if a topic exists.
func TopicExists(topic string) bool {
	_, exists := TopicStateInstance[types.TopicName(topic)]
	return exists
}

// PartitionExists checks if a topic exists.
func PartitionExists(topic string, partition uint32) bool {
	topicState, exists := TopicStateInstance[types.TopicName(topic)]
	if !exists {
		return false
	}
	_, exists = topicState[types.PartitionIndex(partition)]
	return exists
}

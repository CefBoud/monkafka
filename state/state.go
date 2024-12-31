package state

import (
	"github.com/CefBoud/monkafka/types"
)

var Config types.Configuration
var TopicStateInstance types.TopicsState = make(types.TopicsState)
var GroupMetadata = make(map[types.GroupID]map[types.GroupMetadataTopicPartitionKey]types.GroupMetadataTopicPartitionValue)

func GetPartition(topic string, partition uint32) *types.Partition {
	return TopicStateInstance[types.TopicName(topic)][types.PartitionIndex(partition)]
}

func GetPartitions(topic string) []*types.Partition {
	var partitions []*types.Partition
	for _, p := range TopicStateInstance[types.TopicName(topic)] {
		partitions = append(partitions, p)
	}
	return partitions
}

func GroupExists(groupID string) bool {
	_, exists := GroupMetadata[types.GroupID(groupID)]
	return exists
}

func TopicExists(topic string) bool {
	_, exists := TopicStateInstance[types.TopicName(topic)]
	return exists
}

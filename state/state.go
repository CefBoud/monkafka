package state

import (
	"github.com/CefBoud/monkafka/types"
)

var Config types.Configuration
var TopicStateInstance types.TopicsState = make(types.TopicsState)

func GetPartition(topic string, partition uint32) *types.Partition {
	return TopicStateInstance[types.TopicName(topic)][types.PartitionIndex(partition)]
}

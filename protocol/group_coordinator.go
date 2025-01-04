package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

func serializeConsumerOffsetRecord(request *OffsetCommitRequest) [][]byte {
	var result [][]byte

	for _, topic := range request.Topics {
		for _, partition := range topic.Partitions {
			encoder := serde.NewEncoder()
			// https://github.com/apache/kafka/blob/22a9127fe124410faee297d5a8b4f56291a4ad30/group-coordinator/src/main/resources/common/message/OffsetCommitKey.json
			// version prefixed serialization
			encoder.PutInt16(1) // version 1
			// strings's length are se
			encoder.PutString(request.GroupID)
			encoder.PutString(topic.Name)
			encoder.PutInt32(partition.PartitionIndex)
			keyBytes := encoder.Bytes()

			/// kafka/group-coordinator/src/main/resources/common/message/OffsetCommitValue.json
			encoder = serde.NewEncoder()
			// https://github.com/apache/kafka/blob/22a9127fe124410faee297d5a8b4f56291a4ad30/group-coordinator/src/main/resources/common/message/OffsetCommitValue.json
			// version prefixed serialization
			encoder.PutInt16(4) // version 4
			encoder.PutInt64(partition.CommittedOffset)
			encoder.PutInt32(uint32(MinusOne)) // TODO: proper leaderEpoch
			encoder.PutCompactString(partition.CommittedMetadata)
			encoder.PutInt64(utils.NowAsUnixMilli()) // commitTimestamp
			//	encoder.PutInt64(uint64(MinusOne))
			encoder.EndStruct()
			valuesBytes := encoder.Bytes()
			rb := storage.NewRecordBatch(keyBytes, valuesBytes)
			result = append(result, storage.WriteRecordBatch(rb))
		}
	}
	return result
}

func deserializeConsumerOffsetRecord(recordBatchBytes []byte) (types.GroupID, types.GroupMetadataTopicPartitionKey, types.GroupMetadataTopicPartitionValue) {
	recordBatch := storage.ReadRecordBatch(recordBatchBytes)
	record := storage.ReadRecord(recordBatch.Records)

	decoder := serde.NewDecoder(record.Key)
	_ = decoder.UInt16() // version
	groupID := types.GroupID(decoder.String())

	topicPartitionKey := types.GroupMetadataTopicPartitionKey{TopicName: decoder.String(), PartitionIndex: decoder.UInt32()}

	decoder = serde.NewDecoder(record.Value)
	_ = decoder.UInt16() // version
	topicPartitionValue := types.GroupMetadataTopicPartitionValue{
		CommittedOffset:      int64(decoder.UInt64()),
		CommittedLeaderEpoch: int32(decoder.UInt32()),
		Metadata:             decoder.CompactString(),
	}

	// log.Debug("deserializeConsumerOffsetRecord groupID %v, topicPartitionKey %+v, topicPartitionValue %+v", groupID, topicPartitionKey, topicPartitionValue)
	return groupID, topicPartitionKey, topicPartitionValue
}

// GetCommittedOffset returns the committed offset if it exists, otherwise -1
func GetCommittedOffset(groupID string, topic string, partition uint32) int64 {
	state.GetPartition(ConsumerOffsetsTopic, 0).Lock()
	defer state.GetPartition(ConsumerOffsetsTopic, 0).Unlock()

	if groupState, exists := state.GroupMetadata[types.GroupID(groupID)]; exists {
		if metadata, exists2 := groupState[types.GroupMetadataTopicPartitionKey{TopicName: topic, PartitionIndex: partition}]; exists2 {
			return metadata.CommittedOffset
		}
	}
	return -1
}

// UpdateGroupMetadataState  given a __consumer-offsets record, updates the state accordingly
func UpdateGroupMetadataState(recordBytes []byte) {
	// use the ConsumerOffsetsTopic's first partition as a lock to the group state
	state.GetPartition(ConsumerOffsetsTopic, 0).Lock()
	defer state.GetPartition(ConsumerOffsetsTopic, 0).Unlock()

	groupID, topicPartitionKey, topicPartitionValue := deserializeConsumerOffsetRecord(recordBytes)
	log.Debug("UpdateGroupMetadataState groupID: %v key: %+v, value: %+v ", groupID, topicPartitionKey, topicPartitionValue)
	_, exists := state.GroupMetadata[groupID]
	if !exists {
		state.GroupMetadata[groupID] = make(map[types.GroupMetadataTopicPartitionKey]types.GroupMetadataTopicPartitionValue)
	}
	state.GroupMetadata[groupID][topicPartitionKey] = topicPartitionValue

}

// LoadGroupMetadataState from __consumer-offsets metadata
func LoadGroupMetadataState() {
	if !state.TopicExists(ConsumerOffsetsTopic) {
		err := storage.CreateTopic(ConsumerOffsetsTopic, 1)
		if err != nil {
			log.Error("Error creating topic %v. %v ", ConsumerOffsetsTopic, err)
		}
	} else {
		for _, partition := range state.GetPartitions(ConsumerOffsetsTopic) {
			if !partition.IsEmpty() {
				for i := partition.StartOffset(); i <= partition.EndOffset(); i++ {
					recordBytes, _ := storage.GetRecord(i, ConsumerOffsetsTopic, partition.Index)
					UpdateGroupMetadataState(recordBytes)
				}
			}

		}

	}

	log.Info("Loaded GroupMetadataState: %+v", state.GroupMetadata)
}

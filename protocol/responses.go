package protocol

import (
	"os"
	"time"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

// ClusterID should be configurable?
var ClusterID = "MONKAFKA-CLUSTER"

// MinusOne used through variable because setting it as uint directly is rejected
var MinusOne int = -1

// DefaultNumPartition represents the num of partitions during creation if unspecified
// TODO: make configurable
const DefaultNumPartition = 1

// APIVersion (Api key = 18)
func getAPIVersionResponse(req types.Request) []byte {
	response := APIVersionsResponse{
		APIKeys: []APIKey{
			{APIKey: produceKey, MinVersion: 0, MaxVersion: 11},
			{APIKey: fetchKey, MinVersion: 12, MaxVersion: 12},
			{APIKey: listOffsetsKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: metadataKey, MinVersion: 0, MaxVersion: 12},
			{APIKey: offsetCommitKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: offsetFetchKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: findCoordinatorKey, MinVersion: 0, MaxVersion: 6},
			{APIKey: joinGroupKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: heartbeatKey, MinVersion: 0, MaxVersion: 4},
			{APIKey: syncGroupKey, MinVersion: 0, MaxVersion: 5},
			{APIKey: apiVersionKey, MinVersion: 0, MaxVersion: 4},
			{APIKey: createTopicKey, MinVersion: 0, MaxVersion: 7},
			{APIKey: initProducerIDKey, MinVersion: 0, MaxVersion: 5},
		},
	}

	// we are not using EncodeResponseBytes because APIversion doesn't use tagged buffers
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.Encode(response)
	encoder.PutLen()
	return encoder.Bytes()

}

// Metadata	(Api key = 3)
func getMetadataResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	// https://github.com/apache/kafka/blob/430892654bcf45d644e66b532d83aab0f569cb7d/clients/src/main/resources/common/message/MetadataRequest.json#L26-L27
	// An empty array indicates "request metadata for no topics," and a null array is used to
	// indicate "request metadata for all topics."
	// TODO: we are only handling empty and non empty array case, null array (all topics) is not handled
	nbTopics := decoder.CompactArrayLen()

	topics := make([]MetadataResponseTopic, nbTopics)
	topicNameToUUID := make(map[string][16]byte)
	if nbTopics > 0 {
		for i := 0; i < int(nbTopics); i++ {
			uuid := decoder.UUID()
			name := decoder.CompactString()
			topicNameToUUID[name] = uuid
			decoder.EndStruct()
		}
	}

	// Auto create topics if requested
	allowAutoTopicCreation := decoder.Bool()
	for name := range topicNameToUUID {
		if !state.TopicExists(name) {
			if allowAutoTopicCreation {
				err := storage.CreateTopic(name, 1)
				if err != nil {
					log.Error("Error creating topic. %v", err)
				}
			}
		}
	}
	for name, uuid := range topicNameToUUID {
		topic := MetadataResponseTopic{ErrorCode: 0, Name: name, TopicID: uuid, IsInternal: false}
		for partitionIndex := range state.TopicStateInstance[types.TopicName(name)] {
			topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
				ErrorCode:      0,
				PartitionIndex: uint32(partitionIndex),
				LeaderID:       1,
				ReplicaNodes:   []uint32{1},
				IsrNodes:       []uint32{1}})
		}
		topics = append(topics, topic)
	}

	response := MetadataResponse{
		ThrottleTimeMs: 0,
		Brokers: []MetadataResponseBroker{
			{
				NodeID: 1,
				Host:   state.Config.BrokerHost,
				Port:   state.Config.BrokerPort,
				Rack:   ""},
		},
		ClusterID:    ClusterID,
		ControllerID: 1,
		Topics:       topics,
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

// CreateTopics	(Api key = 19)
func getCreateTopicResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactArrayLen() //topicsLen
	topicName := decoder.CompactString()
	numPartitions := decoder.UInt32()

	if int32(numPartitions) == -1 {
		numPartitions = DefaultNumPartition
	}
	response := CreateTopicsResponse{
		Topics: []CreateTopicsResponseTopic{{Name: topicName, TopicID: [16]byte{},
			ErrorCode:         0,
			ErrorMessage:      "",
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
			Configs:           []CreateTopicsResponseConfig{},
		}}}

	err := storage.CreateTopic(topicName, numPartitions)
	if err != nil {
		log.Error("Error creating topic %v", err)
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

// InitProducerID (Api key = 22)
func getInitProducerIDResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactString() //transactionalID
	_ = decoder.UInt32()        //transactionTimeoutMs
	producerID := decoder.UInt64()
	if int(producerID) == -1 {
		producerID = 1 // TODO: set this value properly
	}
	epochID := decoder.UInt16()
	if int16(epochID) == -1 {
		epochID = 1 // TODO: set this value properly
	}

	response := InitProducerIDResponse{
		ProducerID:    producerID,
		ProducerEpoch: epochID,
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

// ReadTopicData Producer (Api key = 0)
func ReadTopicData(producerRequest []byte) []ProduceResponseTopicData {
	var topicData []ProduceResponseTopicData

	decoder := serde.NewDecoder(producerRequest)
	decoder.Offset += 1 + 2 + 4 // no transactional_id + acks +timeout_ms
	nbTopics := decoder.CompactArrayLen()
	for i := 0; i < int(nbTopics); i++ {
		topicName := decoder.CompactString()
		nbPartitions := decoder.CompactArrayLen()
		var partitionData []ProduceResponsePartitionData
		for j := 0; j < int(nbPartitions); j++ {
			index := decoder.UInt32()
			data := decoder.CompactBytes()
			partitionData = append(partitionData, ProduceResponsePartitionData{Index: index, RecordsData: data})
			decoder.EndStruct()
		}
		topicData = append(topicData, ProduceResponseTopicData{Name: topicName, PartitionData: partitionData})
	}
	return topicData
}

func writeProducedRecords(topicData []ProduceResponseTopicData) error {
	for _, td := range topicData {
		for _, pd := range td.PartitionData {
			partitionDir := storage.GetPartitionDir(td.Name, pd.Index)
			err := os.MkdirAll(partitionDir, 0750)
			log.Debug("Writing within partition dir %v", partitionDir)
			if err != nil {
				log.Error("Error creating topic directory: %v", err)
				return err
			}
			err = storage.AppendRecord(td.Name, pd.Index, pd.RecordsData)
			if err != nil {
				log.Error("Error AppendRecord: %v", err)
				return err
			}
		}

	}
	return nil
}
func getProduceResponse(req types.Request) []byte {
	topicData := ReadTopicData(req.Body)
	err := writeProducedRecords(topicData)
	if err != nil {
		log.Error("Error opening partition file: %v", err)
		os.Exit(1)
	}
	response := ProduceResponse{}

	for _, td := range topicData {
		produceTopicResponse := ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.PartitionData {
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, ProducePartitionResponse{Index: pd.Index, LogAppendTimeMs: utils.NowAsUnixMilli(),
				BaseOffset: 0}) //State[td.name+string(pd.index)]
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getFindCoordinatorResponse(req types.Request) []byte {
	// TODO: get requested coordinator keys
	response := FindCoordinatorResponse{
		Coordinators: []FindCoordinatorResponseCoordinator{{
			Key:    "dummy", //"console-consumer-22229",
			NodeID: 1,
			Host:   state.Config.BrokerHost,
			Port:   state.Config.BrokerPort,
		}},
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getJoinGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	groupID := decoder.CompactString()
	decoder.Offset += 4 + 4 //session_timeout_ms + rebalance_timeout_ms
	memberID := decoder.CompactString()
	groupInstanceID := decoder.CompactString()
	protocolType := decoder.CompactString()
	var protocolName []string
	var metadataBytes [][]byte
	for i := 0; i < int(decoder.CompactArrayLen()); i++ {
		protocolName = append(protocolName, decoder.CompactString())
		metadataBytes = append(metadataBytes, decoder.Bytes())
	}

	log.Debug("getJoinGroupResponse: groupID: %v, groupInstanceID:%v, protcolType: %v, protocolName:%v, metadataBytes: %v", groupID, groupInstanceID, protocolType, protocolName, metadataBytes)

	response := JoinGroupResponse{
		GenerationID:   1,
		ProtocolType:   "consumer",
		ProtocolName:   "range",
		Leader:         memberID,
		SkipAssignment: false, // KIP-814 static membership (when false, the consumer group leader will send the assignments)
		MemberID:       memberID,
		Members: []JoinGroupResponseMember{
			{MemberID: memberID,
				Metadata: metadataBytes[0]},
		},
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getHeartbeatResponse(req types.Request) []byte {
	response := HeartbeatResponse{}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getSyncGroupResponse(req types.Request) []byte {
	// get assignment bytes from request
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactString() //groupID
	_ = decoder.UInt32()        //generationId
	_ = decoder.CompactString() //memberID
	_ = decoder.CompactString() //groupInstanceID

	response := SyncGroupResponse{
		ProtocolType: decoder.CompactString(),
		ProtocolName: decoder.CompactString(),
	}
	nbAssignments := decoder.CompactArrayLen()
	// TODO : handle this properly
	for i := 0; i < int(nbAssignments); i++ {
		_ = decoder.CompactString() //memberID
		response.AssignmentBytes = decoder.Bytes()
		decoder.EndStruct()
	}
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getOffsetFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactArrayLen()      // nbGroups
	groupID := decoder.CompactString() //groupID
	_ = decoder.CompactString()        //memberID
	_ = decoder.UInt32()               //memberEpoch
	nbTopics := decoder.CompactArrayLen()
	topicPartitions := make(map[string][]uint32)
	for i := uint64(0); i < nbTopics; i++ {
		topicName := decoder.CompactString()
		topicPartitions[topicName] = make([]uint32, 0)
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			topicPartitions[topicName] = append(topicPartitions[topicName], decoder.UInt32())
		}
		decoder.EndStruct()
	}
	response := OffsetFetchResponse{Groups: []OffsetFetchGroup{
		{GroupID: groupID, Topics: []OffsetFetchTopic{}},
	}}

	for tp, partitions := range topicPartitions {
		offsetFetchTopic := OffsetFetchTopic{Name: tp}
		for _, p := range partitions {
			offsetFetchTopic.Partitions = append(offsetFetchTopic.Partitions, OffsetFetchPartition{
				PartitionIndex:  p,
				CommittedOffset: uint64(GetCommittedOffset(groupID, tp, p)), // -1 triggers a ListOffsets request
			})
		}
		response.Groups[0].Topics = append(response.Groups[0].Topics, offsetFetchTopic)
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getOffsetCommitResponse(req types.Request) []byte {
	offsetCommitRequest := OffsetCommitRequest{}
	decoder := serde.NewDecoder(req.Body)

	offsetCommitRequest.GroupID = decoder.CompactString()
	offsetCommitRequest.GenerationIDOrMemberEpoch = decoder.UInt32()
	offsetCommitRequest.MemberID = decoder.CompactString()
	offsetCommitRequest.GroupInstanceID = decoder.CompactString()

	nbTopics := decoder.CompactArrayLen()
	for i := uint64(0); i < nbTopics; i++ {
		topic := OffsetCommitRequestTopic{}
		topic.Name = decoder.CompactString()
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			topic.Partitions = append(topic.Partitions, OffsetCommitRequestPartition{
				PartitionIndex:       decoder.UInt32(),
				CommittedOffset:      decoder.UInt64(),
				CommittedLeaderEpoch: decoder.UInt32(),
				CommittedMetadata:    decoder.CompactString(),
			})
			decoder.EndStruct()
		}
		offsetCommitRequest.Topics = append(offsetCommitRequest.Topics, topic)
		decoder.EndStruct()
	}

	log.Debug("offsetCommitRequest %+v", offsetCommitRequest)

	for _, recordBytes := range serializeConsumerOffsetRecord(offsetCommitRequest) {
		storage.AppendRecord(ConsumerOffsetsTopic, 0, recordBytes)
		UpdateGroupMetadataState(recordBytes)
	}

	response := OffsetCommitResponse{}
	for _, topic := range offsetCommitRequest.Topics {
		offsetCommitTopic := OffsetCommitResponseTopic{Name: topic.Name}
		for _, p := range topic.Partitions {
			offsetCommitTopic.Partitions = append(offsetCommitTopic.Partitions, OffsetCommitResponsePartition{PartitionIndex: p.PartitionIndex})
		}
		response.Topics = append(response.Topics, offsetCommitTopic)
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	decoder.Offset += 4 + 4 + 4 + 4 + 1 + 4 + 4 // replica_id+ max_wait_ms  + min_bytes +max_bytes +isolation_level + session_id+session_epoch
	nbTopics := decoder.CompactArrayLen()
	type PartitionOffset struct {
		index       uint32
		fetchOffset uint64
	}
	topicPartitions := make(map[string][]PartitionOffset)
	for i := uint64(0); i < nbTopics; i++ {

		topicName := decoder.CompactString()
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			index := decoder.UInt32()
			decoder.Offset += 4 // current_leader_epoch
			fetchOffset := decoder.UInt64()
			decoder.Offset += 4 + 8 + 4 // last_fetched_epoch + log_start_offset + partition_max_bytes
			decoder.EndStruct()
			topicPartitions[topicName] = append(topicPartitions[topicName], PartitionOffset{index: index, fetchOffset: fetchOffset}) //Encoding.Uint32(request[offset:]))
		}
		decoder.EndStruct()
	}
	numTotalRecordBytes := 0
	response := FetchResponse{}
	for tp, partitions := range topicPartitions {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp}
		for _, p := range partitions {
			recordBytes, err := storage.GetRecord(p.fetchOffset, tp, p.index)
			// log.Debug("getFetchResponse GetRecord recordBytes %v", recordBytes)
			numTotalRecordBytes += len(recordBytes)
			if err != nil {
				log.Error("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", uint32(p.fetchOffset), tp, p.index, err)
			}

			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:       p.index,
					HighWatermark:        state.GetPartition(tp, p.index).EndOffset(), //uint64(MinusOne),
					LastStableOffset:     uint64(MinusOne),
					LogStartOffset:       state.GetPartition(tp, p.index).StartOffset(),
					PreferredReadReplica: 1,
					Records:              recordBytes, // []byte(line),
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	if numTotalRecordBytes == 0 {
		log.Info("There is no data available for this fetch request, waiting for a bit ..")
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getListOffsetsResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	listOffsetsRequest := ListOffsetsRequest{
		ReplicaID:      decoder.UInt32(),
		IsolationLevel: decoder.UInt8(),
	}

	numTopics := decoder.CompactArrayLen()

	for i := uint64(0); i < numTopics; i++ {
		topic := ListOffsetsRequestTopic{
			Name: decoder.CompactString(),
		}
		numPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < numPartitions; j++ {

			topic.Partitions = append(topic.Partitions, ListOffsetsRequestPartition{
				PartitionIndex:     decoder.UInt32(),
				CurrentLeaderEpoch: decoder.UInt32(),
				Timestamp:          decoder.UInt64(),
			})
			decoder.EndStruct()
		}
		decoder.EndStruct()
		listOffsetsRequest.Topics = append(listOffsetsRequest.Topics, topic)
	}
	log.Debug("listOffsetsRequest %+v", listOffsetsRequest)

	response := ListOffsetsResponse{}
	for _, t := range listOffsetsRequest.Topics {
		topic := ListOffsetsResponseTopic{Name: t.Name}
		for _, p := range t.Partitions {
			partition := ListOffsetsResponsePartition{PartitionIndex: p.PartitionIndex, LeaderEpoch: uint32(MinusOne)}
			if p.Timestamp == uint64(ListOffsetsEarliestTimestamp) {
				partition.Offset = state.GetPartition(t.Name, p.PartitionIndex).StartOffset()
			} else if p.Timestamp == uint64(ListOffsetsLatestTimestamp) {
				partition.Offset = state.GetPartition(t.Name, p.PartitionIndex).EndOffset()
				partition.Timestamp = state.GetPartition(t.Name, p.PartitionIndex).ActiveSegment().MaxTimestamp
			} else {
				// TODO: implement ListOffsetsMaxTimestamp
				log.Error("ListOffsetsMaxTimestamp not implemented!")
				os.Exit(1)
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		response.Topics = append(response.Topics, topic)
	}
	log.Debug("ListOffsetsResponse %+v", response)

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

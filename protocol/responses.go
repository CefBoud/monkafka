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
	APIVersions := APIVersionsResponse{
		ErrorCode: 0,
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

	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.PutInt16(APIVersions.ErrorCode)
	encoder.PutInt32(uint32(len(APIVersions.APIKeys)))
	for _, k := range APIVersions.APIKeys {
		encoder.PutInt16(k.APIKey)
		encoder.PutInt16(k.MinVersion)
		encoder.PutInt16(k.MaxVersion)
	}
	encoder.PutInt32(0) //ThrottleTime
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
		for partitionIndex := range state.TopicStateInstance[types.TopicName(name)] {
			topics = append(topics, MetadataResponseTopic{ErrorCode: 0, Name: name, TopicID: uuid, IsInternal: false, Partitions: []MetadataResponsePartition{{
				ErrorCode:      0,
				PartitionIndex: uint32(partitionIndex),
				LeaderID:       1,
				ReplicaNodes:   []uint32{1},
				IsrNodes:       []uint32{1}},
			}})
		}
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // header end

	encoder.PutInt32(uint32(response.ThrottleTimeMs))

	// brokers
	encoder.PutCompactArrayLen(len(response.Brokers))
	// Encoding.PutUint32(b[offset:], uint32(len(response.brokers)))
	// offset += 4
	for _, bk := range response.Brokers {
		encoder.PutInt32(uint32(bk.NodeID))
		encoder.PutCompactString(bk.Host)
		encoder.PutInt32(uint32(bk.Port))
		encoder.PutCompactString(bk.Rack) // compact nullable string
		encoder.EndStruct()
	}
	// cluster id compact_string
	encoder.PutCompactString(response.ClusterID)
	encoder.PutInt32(uint32(response.ControllerID))
	// topics compact_array
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutInt16(uint16(tp.ErrorCode))
		encoder.PutCompactString(tp.Name)
		encoder.PutBytes(tp.TopicID[:])
		encoder.PutBool(tp.IsInternal)
		encoder.PutCompactArrayLen(len(tp.Partitions))

		for _, par := range tp.Partitions {
			encoder.PutInt16(uint16(par.ErrorCode))
			encoder.PutInt32(uint32(par.PartitionIndex))
			encoder.PutInt32(uint32(par.LeaderID))
			// replicas
			encoder.PutCompactArrayLen(len(par.ReplicaNodes))
			for _, rn := range par.ReplicaNodes {
				encoder.PutInt32(uint32(rn))
			}
			// isrs
			encoder.PutCompactArrayLen(len(par.IsrNodes))
			for _, isr := range par.ReplicaNodes {
				encoder.PutInt32(uint32(isr))
			}
			encoder.PutCompactArrayLen(len(par.OfflineReplicas))
			for _, off := range par.OfflineReplicas {
				encoder.PutInt32(uint32(off))
			}
			encoder.EndStruct()
		}

		encoder.PutInt32(tp.TopicAuthorizedOperations)
		encoder.EndStruct() // end topic
	}
	return encoder.FinishAndReturn()
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
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms

	// topics
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutCompactString(tp.Name)
		encoder.PutBytes(tp.TopicID[:]) // UUID
		encoder.PutInt16(tp.ErrorCode)
		encoder.PutCompactString(tp.ErrorMessage)
		encoder.PutInt32(tp.NumPartitions)
		encoder.PutInt16(tp.ReplicationFactor)

		encoder.PutCompactArrayLen(len(tp.Configs)) // empty config array
		encoder.EndStruct()
	}

	err := storage.CreateTopic(topicName, numPartitions)
	if err != nil {
		log.Error("Error creating topic %v", err)
	}
	return encoder.FinishAndReturn()
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.ThrottleTimeMs)
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt64(response.ProducerID)
	encoder.PutInt16(response.ProducerEpoch)
	return encoder.FinishAndReturn()
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

	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	for _, td := range topicData {
		produceTopicResponse := ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.PartitionData {
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, ProducePartitionResponse{Index: pd.Index, LogAppendTimeMs: utils.NowAsUnixMilli(),
				BaseOffset: 0}) //State[td.name+string(pd.index)]
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}

	encoder.PutCompactArrayLen(len(response.ProduceTopicResponses))
	for _, topicResp := range response.ProduceTopicResponses {
		encoder.PutCompactString(topicResp.Name)
		encoder.PutCompactArrayLen(len(topicResp.ProducePartitionResponses))
		for _, partitionResp := range topicResp.ProducePartitionResponses {
			encoder.PutInt32(partitionResp.Index)
			encoder.PutInt16(partitionResp.ErrorCode)
			encoder.PutInt64(partitionResp.BaseOffset)
			encoder.PutInt64(partitionResp.LogAppendTimeMs)
			encoder.PutInt64(partitionResp.LogStartOffset)
			encoder.PutCompactArrayLen(len(partitionResp.RecordErrors))
			// TODO add record errors
			encoder.PutCompactString(partitionResp.ErrorMessage)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}

	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	return encoder.FinishAndReturn()
}

func getFindCoordinatorResponse(req types.Request) []byte {
	// TODO: get requested coordinator keys
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(0) // throttle_time_ms
	// TODO: populate this properly
	coordinators := []FindCoordinatorResponseCoordinator{{
		Key:    "dummy", //"console-consumer-22229",
		NodeID: 1,
		Host:   state.Config.BrokerHost,
		Port:   state.Config.BrokerPort,
	}}
	encoder.PutCompactArrayLen(len(coordinators))
	for _, c := range coordinators {
		encoder.PutCompactString(c.Key)
		encoder.PutInt32(c.NodeID)
		encoder.PutCompactString(c.Host)
		encoder.PutInt32(c.Port)
		encoder.PutInt16(c.ErrorCode)
		encoder.PutCompactString(c.ErrorMessage)
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(response.ThrottleTimeMS) // throttle_time_ms
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.GenerationID)
	encoder.PutCompactString(response.ProtocolType)
	encoder.PutCompactString(response.ProtocolName)
	encoder.PutCompactString(response.Leader)
	encoder.PutBool(response.SkipAssignment)
	encoder.PutCompactString(response.MemberID)
	encoder.PutCompactArrayLen(len(response.Members))
	for _, m := range response.Members {
		encoder.PutCompactString(m.MemberID)
		// encoder.PutCompactArrayLen(-1)
		encoder.PutCompactString(m.GroupInstanceID)
		encoder.PutCompactArrayLen(len(m.Metadata))
		encoder.PutBytes(m.Metadata)
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
}

func getHeartbeatResponse(req types.Request) []byte {
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(0) //throttle_time_ms
	encoder.PutInt16(0) //error_code
	return encoder.FinishAndReturn()
}

func getSyncGroupResponse(req types.Request) []byte {
	// get assignment bytes from request
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactString() //groupID
	_ = decoder.UInt32()        //generationId
	_ = decoder.CompactString() //memberID
	_ = decoder.CompactString() //groupInstanceID
	protocolType := decoder.CompactString()
	protocolName := decoder.CompactString()
	nbAssignments := decoder.CompactArrayLen()
	assignmentBytes := make([][]byte, nbAssignments) // TODO : handle this properly
	for i := 0; i < int(nbAssignments); i++ {
		_ = decoder.CompactString() //memberID
		assignmentBytes[i] = decoder.Bytes()
		decoder.EndStruct()
	}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(0) //throttle_time_ms
	encoder.PutInt16(0) //error_code

	encoder.PutCompactString(protocolType)
	encoder.PutCompactString(protocolName)

	// assignments COMPACT_BYTES
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder.PutCompactArrayLen(len(assignmentBytes[0]))
	encoder.PutBytes(assignmentBytes[0])
	// end assignments
	return encoder.FinishAndReturn()
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	encoder.PutCompactArrayLen(len(response.Groups))
	for _, g := range response.Groups {
		encoder.PutCompactString(g.GroupID)
		encoder.PutCompactArrayLen(len(g.Topics))
		for _, t := range g.Topics {
			encoder.PutCompactString(t.Name)
			encoder.PutCompactArrayLen(len(t.Partitions))
			for _, p := range t.Partitions {
				encoder.PutInt32(p.PartitionIndex)
				encoder.PutInt64(p.CommittedOffset)
				encoder.PutInt32(p.CommittedLeaderEpoch)
				encoder.PutCompactString(p.Metadata)
				encoder.PutInt16(p.ErrorCode)
				encoder.EndStruct()
			}
			encoder.PutInt16(t.ErrorCode)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, t := range response.Topics {
		encoder.PutCompactString(t.Name)
		encoder.PutCompactArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			encoder.PutInt32(p.PartitionIndex)
			encoder.PutInt16(p.ErrorCode)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.ThrottleTimeMs)
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.SessionID)
	encoder.PutCompactArrayLen(len(response.Responses))
	for _, r := range response.Responses {
		encoder.PutCompactString(r.TopicName)
		encoder.PutCompactArrayLen(len(r.Partitions))
		for _, p := range r.Partitions {
			encoder.PutInt32(p.PartitionIndex)
			encoder.PutInt16(p.ErrorCode)
			encoder.PutInt64(p.HighWatermark)
			encoder.PutInt64(p.LastStableOffset)
			encoder.PutInt64(p.LogStartOffset)

			encoder.PutCompactArrayLen(len(p.AbortedTransactions))
			encoder.PutInt32(p.PreferredReadReplica)

			if len(p.Records) > 0 {
				encoder.PutCompactBytes(p.Records)
			} else {
				encoder.PutCompactArrayLen(len(p.Records))
			}

			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
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
	// log.Debug("listOffsetsRequest %+v", listOffsetsRequest)

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
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.ThrottleTimeMs)
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, t := range response.Topics {
		encoder.PutCompactString(t.Name)
		encoder.PutCompactArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			encoder.PutInt32(p.PartitionIndex)
			encoder.PutInt16(p.ErrorCode)
			encoder.PutInt64(p.Timestamp)
			encoder.PutInt64(p.Offset)
			encoder.PutInt32(p.LeaderEpoch)
			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
}

package protocol

import (
	"log"
	"os"
	"time"

	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

var CLUSTER_ID = "MONKAFKA-CLUSTER"
var MINUS_ONE int = -1

const DEFAULT_NUM_PARTITION = 1

// APIVersion (Api key = 18)
func getAPIVersionResponse(req types.Request) []byte {
	APIVersions := types.APIVersionsResponse{
		ErrorCode: 0,
		ApiKeys: []types.APIKey{
			{ApiKey: ProduceKey, MinVersion: 0, MaxVersion: 11},
			{ApiKey: FetchKey, MinVersion: 12, MaxVersion: 12},
			{ApiKey: MetadataKey, MinVersion: 0, MaxVersion: 12},
			{ApiKey: OffsetFetchKey, MinVersion: 0, MaxVersion: 9},
			{ApiKey: FindCoordinatorKey, MinVersion: 0, MaxVersion: 6},
			{ApiKey: JoinGroupKey, MinVersion: 0, MaxVersion: 9},
			{ApiKey: HeartbeatKey, MinVersion: 0, MaxVersion: 4},
			{ApiKey: SyncGroupKey, MinVersion: 0, MaxVersion: 5},
			{ApiKey: APIVersionKey, MinVersion: 0, MaxVersion: 4},
			{ApiKey: CreateTopicKey, MinVersion: 0, MaxVersion: 7},
			{ApiKey: InitProducerIdKey, MinVersion: 0, MaxVersion: 5},
		},
	}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.PutInt16(APIVersions.ErrorCode)
	encoder.PutInt32(uint32(len(APIVersions.ApiKeys)))
	for _, k := range APIVersions.ApiKeys {
		encoder.PutInt16(k.ApiKey)
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

	topics := make([]types.MetadataResponseTopic, nbTopics)
	topicNameToUUID := make(map[string][16]byte)
	if nbTopics > 0 {
		for i := 0; i < int(nbTopics); i++ {
			uuid := decoder.UUID()
			name := decoder.String()
			topicNameToUUID[name] = uuid
			decoder.EndStruct()
		}
	}

	// Auto create topics if requested
	allowAutoTopicCreation := decoder.Bool()
	log.Println("allowAutoTopicCreation", allowAutoTopicCreation, topicNameToUUID)
	for name := range topicNameToUUID {
		if _, exists := state.TopicStateInstance[types.TopicName(name)]; !exists {
			if allowAutoTopicCreation {
				err := storage.CreateTopic(name, 1)
				if err != nil {
					log.Println("Error creating topic ", err)
				}
			}
		}
	}
	for name, uuid := range topicNameToUUID {
		for partitionIndex := range state.TopicStateInstance[types.TopicName(name)] {
			topics = append(topics, types.MetadataResponseTopic{Error_code: 0, Name: name, Topic_id: uuid, Is_internal: false, Partitions: []types.MetadataResponsePartition{{
				Error_code:      0,
				Partition_index: uint32(partitionIndex),
				Leader_id:       1,
				Replica_nodes:   []uint32{1},
				Isr_nodes:       []uint32{1}},
			}})
		}
	}

	response := types.MetadataResponse{
		Throttle_time_ms: 0,
		Brokers: []types.MetadataResponseBroker{
			{
				Node_id: 1,
				Host:    state.Config.BrokerHost,
				Port:    state.Config.BrokerPort,
				Rack:    ""},
		},
		Cluster_id:    CLUSTER_ID,
		Controller_id: 1,
		Topics:        topics,
	}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // header end

	encoder.PutInt32(uint32(response.Throttle_time_ms))

	// brokers
	encoder.PutCompactArrayLen(len(response.Brokers))
	// Encoding.PutUint32(b[offset:], uint32(len(response.brokers)))
	// offset += 4
	for _, bk := range response.Brokers {
		encoder.PutInt32(uint32(bk.Node_id))
		encoder.PutString(bk.Host)
		encoder.PutInt32(uint32(bk.Port))
		encoder.PutString(bk.Rack) // compact nullable string
		encoder.EndStruct()
	}
	// cluster id compact_string
	encoder.PutString(response.Cluster_id)
	encoder.PutInt32(uint32(response.Controller_id))
	// topics compact_array
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutInt16(uint16(tp.Error_code))
		encoder.PutString(tp.Name)
		encoder.PutBytes(tp.Topic_id[:])
		encoder.PutBool(tp.Is_internal)
		encoder.PutCompactArrayLen(len(tp.Partitions))

		for _, par := range tp.Partitions {
			encoder.PutInt16(uint16(par.Error_code))
			encoder.PutInt32(uint32(par.Partition_index))
			encoder.PutInt32(uint32(par.Leader_id))
			// replicas
			encoder.PutCompactArrayLen(len(par.Replica_nodes))
			for _, rn := range par.Replica_nodes {
				encoder.PutInt32(uint32(rn))
			}
			// isrs
			encoder.PutCompactArrayLen(len(par.Isr_nodes))
			for _, isr := range par.Replica_nodes {
				encoder.PutInt32(uint32(isr))
			}
			encoder.PutCompactArrayLen(len(par.Offline_replicas))
			for _, off := range par.Offline_replicas {
				encoder.PutInt32(uint32(off))
			}
			encoder.EndStruct()
		}

		encoder.PutInt32(tp.Topic_authorized_operations)
		encoder.EndStruct() // end topic
	}
	return encoder.FinishAndReturn()
}

// CreateTopics	(Api key = 19)
func getCreateTopicResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactArrayLen() //topicsLen
	topicName := decoder.String()
	numPartitions := decoder.UInt32()

	if int32(numPartitions) == -1 {
		numPartitions = DEFAULT_NUM_PARTITION
	}
	response := types.CreateTopicsResponse{
		Topics: []types.CreateTopicsResponseTopic{{Name: topicName, TopicID: [16]byte{},
			ErrorCode:         0,
			ErrorMessage:      "",
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
			Configs:           []types.CreateTopicsResponseConfig{},
		}}}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms

	// topics
	encoder.PutCompactArrayLen(len(response.Topics))
	for _, tp := range response.Topics {
		encoder.PutString(tp.Name)
		encoder.PutBytes(tp.TopicID[:]) // UUID
		encoder.PutInt16(tp.ErrorCode)
		encoder.PutString(tp.ErrorMessage)
		encoder.PutInt32(tp.NumPartitions)
		encoder.PutInt16(tp.ReplicationFactor)

		encoder.PutCompactArrayLen(len(tp.Configs)) // empty config array
		encoder.EndStruct()
	}

	err := storage.CreateTopic(topicName, numPartitions)
	if err != nil {
		log.Println("Error creating topic ", err)
	}
	return encoder.FinishAndReturn()
}

// InitProducerId (Api key = 22)
func getInitProducerIdResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.String() //transactional_id
	_ = decoder.UInt32() //transactionTimeoutMs
	producerId := decoder.UInt64()
	if int(producerId) == -1 {
		producerId = 1 // TODO: set this value properly
	}
	epochId := decoder.UInt16()
	if int16(epochId) == -1 {
		epochId = 1 // TODO: set this value properly
	}

	response := types.InitProducerIdResponse{
		Producer_id:    producerId,
		Producer_epoch: epochId,
	}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.Throttle_time_ms)
	encoder.PutInt16(response.Error_code)
	encoder.PutInt64(response.Producer_id)
	encoder.PutInt16(response.Producer_epoch)
	return encoder.FinishAndReturn()
}

// Producer (Api key = 0)
func ReadTopicData(producerRequest []byte) []types.ProduceResponseTopicData {
	var topic_data []types.ProduceResponseTopicData

	decoder := serde.NewDecoder(producerRequest)
	decoder.Offset += 1 + 2 + 4 // no transactional_id + acks +timeout_ms
	nbTopics := decoder.CompactArrayLen()
	for i := 0; i < int(nbTopics); i++ {
		topicName := decoder.String()
		nbPartitions := decoder.CompactArrayLen()
		var partition_data []types.ProduceResponsePartitionData
		for j := 0; j < int(nbPartitions); j++ {
			index := decoder.UInt32()
			data := decoder.BytesWithLen()
			partition_data = append(partition_data, types.ProduceResponsePartitionData{Index: index, RecordsData: data})
			decoder.EndStruct()
		}
		topic_data = append(topic_data, types.ProduceResponseTopicData{Name: topicName, Partition_data: partition_data})
	}
	return topic_data
}

func writeProducedRecords(topic_data []types.ProduceResponseTopicData) error {
	for _, td := range topic_data {
		for _, pd := range td.Partition_data {
			partitionDir := storage.GetPartitionDir(td.Name, pd.Index)
			err := os.MkdirAll(partitionDir, 0750)
			log.Println("Writing within partition dir ", partitionDir)
			if err != nil {
				log.Println("Error creating topic directory:", err)
				return err
			}
			err = storage.AppendRecord(td.Name, pd.Index, pd.RecordsData)
			if err != nil {
				log.Println("Error AppendRecord:", err)
				return err
			}
			// log.Printf("produce len(recordsByte) %+v  ParseRecord %+v \n\n", len(pd.RecordsData), storage.ParseRecord(pd.RecordsData))
		}

	}
	return nil
}
func getProduceResponse(req types.Request) []byte {
	topic_data := ReadTopicData(req.Body)
	err := writeProducedRecords(topic_data)
	if err != nil {
		log.Println("Error opening partition file:", err)
		os.Exit(1)
	}
	response := types.ProduceResponse{}

	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	for _, td := range topic_data {
		produceTopicResponse := types.ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.Partition_data {
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, types.ProducePartitionResponse{Index: pd.Index, LogAppendTimeMs: utils.NowAsUnixMilli(),
				BaseOffset: 0}) //State[td.name+string(pd.index)]
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}

	encoder.PutCompactArrayLen(len(response.ProduceTopicResponses))
	for _, topicResp := range response.ProduceTopicResponses {
		encoder.PutString(topicResp.Name)
		encoder.PutCompactArrayLen(len(topicResp.ProducePartitionResponses))
		for _, partitionResp := range topicResp.ProducePartitionResponses {
			encoder.PutInt32(partitionResp.Index)
			encoder.PutInt16(partitionResp.ErrorCode)
			encoder.PutInt64(partitionResp.BaseOffset)
			encoder.PutInt64(partitionResp.LogAppendTimeMs)
			encoder.PutInt64(partitionResp.LogStartOffset)
			encoder.PutCompactArrayLen(len(partitionResp.RecordErrors))
			// TODO add record errors
			encoder.PutString(partitionResp.ErrorMessage)
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
	coordinators := []types.FindCoordinatorResponseCoordinator{{
		Key:    "dummy", //"console-consumer-22229",
		NodeID: 1,
		Host:   state.Config.BrokerHost,
		Port:   state.Config.BrokerPort,
	}}
	encoder.PutCompactArrayLen(len(coordinators))
	for _, c := range coordinators {
		encoder.PutString(c.Key)
		encoder.PutInt32(c.NodeID)
		encoder.PutString(c.Host)
		encoder.PutInt32(c.Port)
		encoder.PutInt16(c.ErrorCode)
		encoder.PutString(c.ErrorMessage)
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
}

func getJoinGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	groupId := decoder.String()
	decoder.Offset += 4 + 4 //session_timeout_ms + rebalance_timeout_ms
	memberId := decoder.String()
	groupInstanceId := decoder.String()
	protocolType := decoder.String()
	var protocolName []string
	var metadataBytes [][]byte
	for i := 0; i < int(decoder.CompactArrayLen()); i++ {
		protocolName = append(protocolName, decoder.String())
		metadataBytes = append(metadataBytes, decoder.Bytes())
	}

	log.Printf("getJoinGroupResponse: groupId: %v, groupInstanceId:%v, protcolType: %v, protocolName:%v, metadataBytes: %v", groupId, groupInstanceId, protocolType, protocolName, metadataBytes)

	response := types.JoinGroupResponse{
		GenerationID:   1,
		ProtocolType:   "consumer",
		ProtocolName:   "range",
		Leader:         memberId,
		SkipAssignment: false, // KIP-814 static membership (when false, the consumer group leader will send the assignments)
		MemberID:       memberId,
		Members: []types.JoinGroupResponseMember{
			{MemberID: memberId,
				Metadata: metadataBytes[0]},
		},
	}
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(response.ThrottleTimeMS) // throttle_time_ms
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.GenerationID)
	encoder.PutString(response.ProtocolType)
	encoder.PutString(response.ProtocolName)
	encoder.PutString(response.Leader)
	encoder.PutBool(response.SkipAssignment)
	encoder.PutString(response.MemberID)
	encoder.PutCompactArrayLen(len(response.Members))
	for _, m := range response.Members {
		encoder.PutString(m.MemberID)
		// encoder.PutCompactArrayLen(-1)
		encoder.PutString(m.GroupInstanceID)
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
	_ = decoder.String() //groupId
	_ = decoder.UInt32() //generationId
	_ = decoder.String() //memberId
	_ = decoder.String() //groupInstanceId
	protocolType := decoder.String()
	protocolName := decoder.String()
	nbAssignments := decoder.CompactArrayLen()
	assignmentBytes := make([][]byte, nbAssignments) // TODO : handle this properly
	for i := 0; i < int(nbAssignments); i++ {
		_ = decoder.String() //memberId
		assignmentBytes[i] = decoder.Bytes()
		decoder.EndStruct()
	}
	// log.Println("assignmentBytes", assignmentBytes)
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header

	encoder.PutInt32(0) //throttle_time_ms
	encoder.PutInt16(0) //error_code

	encoder.PutString(protocolType)
	encoder.PutString(protocolName)

	// assignments COMPACT_BYTES
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder.PutCompactArrayLen(len(assignmentBytes[0]))
	encoder.PutBytes(assignmentBytes[0])
	// end assignments
	return encoder.FinishAndReturn()
}
func getOffsetFetchResponse(req types.Request) []byte {
	// TODO: implement __consumer_offsets
	decoder := serde.NewDecoder(req.Body)
	_ = decoder.CompactArrayLen() // nbGroups
	groupId := decoder.String()   //groupId
	_ = decoder.String()          //memberId
	_ = decoder.UInt32()          //memberEpoch
	nbTopics := decoder.CompactArrayLen()
	topic_partitions := make(map[string][]uint32)
	for i := uint64(0); i < nbTopics; i++ {
		topicName := decoder.String()
		topic_partitions[topicName] = make([]uint32, 0)
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			topic_partitions[topicName] = append(topic_partitions[topicName], decoder.UInt32())
		}
		decoder.EndStruct()
	}
	// log.Println("topic_partitions", topic_partitions)
	response := types.OffsetFetchResponse{Groups: []types.OffsetFetchGroup{
		{GroupID: groupId, Topics: []types.OffsetFetchTopic{}},
	}}
	for tp, partitions := range topic_partitions {
		offsetFetchTopic := types.OffsetFetchTopic{Name: tp}
		for _, p := range partitions {
			offsetFetchTopic.Partitions = append(offsetFetchTopic.Partitions, types.OffsetFetchPartition{PartitionIndex: p})
		}
		response.Groups[0].Topics = append(response.Groups[0].Topics, offsetFetchTopic)
	}

	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct()                       // end header
	encoder.PutInt32(response.ThrottleTimeMs) // throttle_time_ms
	encoder.PutCompactArrayLen(len(response.Groups))
	for _, g := range response.Groups {
		encoder.PutString(g.GroupID)
		encoder.PutCompactArrayLen(len(g.Topics))
		for _, t := range g.Topics {
			encoder.PutString(t.Name)
			encoder.PutCompactArrayLen(len(t.Partitions))
			for _, p := range t.Partitions {
				encoder.PutInt32(p.PartitionIndex)
				encoder.PutInt64(p.CommittedOffset)
				encoder.PutInt32(p.CommittedLeaderEpoch)
				encoder.PutString(p.Metadata)
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

func getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	decoder.Offset += 4 + 4 + 4 + 4 + 1 + 4 + 4 // replica_id+ max_wait_ms  + min_bytes +max_bytes +isolation_level + session_id+session_epoch
	nbTopics := decoder.CompactArrayLen()
	type PartitionOffset struct {
		index       uint32
		fetchOffset uint64
	}
	topic_partitions := make(map[string][]PartitionOffset)
	for i := uint64(0); i < nbTopics; i++ {

		topicName := decoder.String()
		// log.Println("topicName", topicName)
		nbPartitions := decoder.CompactArrayLen()
		for j := uint64(0); j < nbPartitions; j++ {
			index := decoder.UInt32()
			decoder.Offset += 4 // current_leader_epoch
			fetchOffset := decoder.UInt64()
			decoder.Offset += 4 + 8 + 4 // last_fetched_epoch + log_start_offset + partition_max_bytes
			decoder.EndStruct()
			topic_partitions[topicName] = append(topic_partitions[topicName], PartitionOffset{index: index, fetchOffset: fetchOffset}) //Encoding.Uint32(request[offset:]))
		}
		decoder.EndStruct()
	}
	numTotalRecordBytes := 0
	response := types.FetchResponse{}
	for tp, partitions := range topic_partitions {
		fetchTopicResponse := types.FetchTopicResponse{TopicName: tp}
		for _, p := range partitions {
			recordBytes, err := storage.GetRecord(uint32(p.fetchOffset), tp, p.index)
			numTotalRecordBytes += len(recordBytes)
			if err != nil {
				log.Printf("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", uint32(p.fetchOffset), tp, p.index, err)
			}
			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				types.FetchPartitionResponse{
					PartitionIndex:       p.index,
					HighWatermark:        uint64(MINUS_ONE), //uint64(MINUS_ONE),
					LastStableOffset:     uint64(MINUS_ONE),
					LogStartOffset:       0,
					PreferredReadReplica: 1,
					Records:              recordBytes, // []byte(line),
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	if numTotalRecordBytes == 0 {
		log.Println("There is no data available for this fetch request, waiting for a bit ..")
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	}
	// log.Println("FetchResponse", response)
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.EndStruct() // end header
	encoder.PutInt32(response.ThrottleTimeMs)
	encoder.PutInt16(response.ErrorCode)
	encoder.PutInt32(response.SessionId)
	encoder.PutCompactArrayLen(len(response.Responses))
	for _, r := range response.Responses {
		encoder.PutString(r.TopicName)
		encoder.PutCompactArrayLen(len(r.Partitions))
		for _, p := range r.Partitions {
			encoder.PutInt32(p.PartitionIndex)
			encoder.PutInt16(p.ErrorCode)
			encoder.PutInt64(p.HighWatermark)
			encoder.PutInt64(p.LastStableOffset)
			encoder.PutInt64(p.LogStartOffset)

			encoder.PutCompactArrayLen(len(p.AbortedTransactions))
			encoder.PutInt32(p.PreferredReadReplica)
			// length is already included in p.Records if defined. If not, we set it ourselves
			if len(p.Records) > 0 {
				encoder.PutBytes(p.Records)
			} else {
				encoder.PutCompactArrayLen(len(p.Records))
			}

			encoder.EndStruct()
		}
		encoder.EndStruct()
	}
	return encoder.FinishAndReturn()
}

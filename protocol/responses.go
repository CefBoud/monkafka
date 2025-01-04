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
	metadataRequest := decoder.Decode(&MetadataRequest{}).(*MetadataRequest)
	log.Debug("metadataRequest %+v", metadataRequest)

	topics := make([]MetadataResponseTopic, len(metadataRequest.Topics))

	for _, topic := range metadataRequest.Topics {
		topic := MetadataResponseTopic{ErrorCode: 0, Name: topic.Name, TopicID: topic.TopicID, IsInternal: false}
		for partitionIndex := range state.TopicStateInstance[types.TopicName(topic.Name)] {
			topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
				ErrorCode:      0,
				PartitionIndex: uint32(partitionIndex),
				LeaderID:       1,
				ReplicaNodes:   []uint32{1},
				IsrNodes:       []uint32{1}})
		}
		topics = append(topics, topic)

		if !state.TopicExists(topic.Name) {
			if metadataRequest.AllowAutoTopicCreation {
				err := storage.CreateTopic(topic.Name, 1)
				if err != nil {
					log.Error("Error creating topic. %v", err)
				}
			}
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
	return encoder.EncodeResponseBytes(req, response)
}

// CreateTopics	(Api key = 19)
func getCreateTopicResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	createTopicsRequest := decoder.Decode(&CreateTopicsRequest{}).(*CreateTopicsRequest)
	log.Debug("CreateTopicsRequest %+v", createTopicsRequest)

	response := CreateTopicsResponse{}
	for _, topic := range createTopicsRequest.Topics {
		if int32(topic.NumPartitions) == -1 {
			topic.NumPartitions = DefaultNumPartition
		}
		response.Topics = append(response.Topics, CreateTopicsResponseTopic{
			Name:              topic.Name,
			TopicID:           [16]byte{},
			ErrorCode:         0,
			ErrorMessage:      "",
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
			Configs:           []CreateTopicsResponseConfig{}, // TODO handle conf
		})
		err := storage.CreateTopic(topic.Name, topic.NumPartitions)
		if err != nil {
			log.Error("Error creating topic %v", err)
		}
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

// InitProducerID (Api key = 22)
func getInitProducerIDResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	initProducerIDRequest := decoder.Decode(&InitProducerIDRequest{}).(*InitProducerIDRequest)
	log.Debug(" initProducerIDRequest %+v", initProducerIDRequest)

	if int(initProducerIDRequest.ProducerID) == -1 {
		initProducerIDRequest.ProducerID = 1 // TODO: set this value properly
	}
	if int16(initProducerIDRequest.ProducerEpoch) == -1 {
		initProducerIDRequest.ProducerEpoch = 1 // TODO: set this value properly
	}

	response := InitProducerIDResponse{
		ProducerID:    initProducerIDRequest.ProducerID,
		ProducerEpoch: initProducerIDRequest.ProducerEpoch,
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func writeProducedRecords(produceRequest *ProduceRequest) error {
	for _, td := range produceRequest.TopicData {
		for _, pd := range td.PartitionData {
			partitionDir := storage.GetPartitionDir(td.Name, pd.Index)
			err := os.MkdirAll(partitionDir, 0750)
			log.Debug("Writing within partition dir %v", partitionDir)
			if err != nil {
				log.Error("Error creating topic directory: %v", err)
				return err
			}
			err = storage.AppendRecord(td.Name, pd.Index, pd.Records)
			if err != nil {
				log.Error("Error AppendRecord: %v", err)
				return err
			}
		}
	}
	return nil
}
func getProduceResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	produceRequest := decoder.Decode(&ProduceRequest{}).(*ProduceRequest)
	log.Debug("ProduceRequest %+v", produceRequest)

	err := writeProducedRecords(produceRequest)
	if err != nil {
		log.Error("Error opening partition file: %v", err)
		os.Exit(1)
	}
	response := ProduceResponse{}

	for _, td := range produceRequest.TopicData {
		produceTopicResponse := ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.PartitionData {
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, ProducePartitionResponse{Index: pd.Index, LogAppendTimeMs: utils.NowAsUnixMilli(),
				BaseOffset: 0}) // TODO: should this be 0? probably not..
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
	joinGroupRequest := decoder.Decode(&JoinGroupRequest{}).(*JoinGroupRequest)
	log.Debug("joinGroupRequest %+v", joinGroupRequest)

	response := JoinGroupResponse{
		GenerationID:   1,
		ProtocolType:   joinGroupRequest.ProtocolType,
		ProtocolName:   joinGroupRequest.Protocols[0].Name,
		Leader:         joinGroupRequest.MemberID,
		SkipAssignment: false, // KIP-814 static membership (when false, the consumer group leader will send the assignments)
		MemberID:       joinGroupRequest.MemberID,
		Members: []JoinGroupResponseMember{
			{MemberID: joinGroupRequest.MemberID,
				Metadata: joinGroupRequest.Protocols[0].Metadata},
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
	decoder := serde.NewDecoder(req.Body)
	syncGroupRequest := decoder.Decode(&SyncGroupRequest{}).(*SyncGroupRequest)

	log.Info("SyncGroupRequest %+v", syncGroupRequest)
	response := SyncGroupResponse{
		ProtocolType:    syncGroupRequest.ProtocolType,
		ProtocolName:    syncGroupRequest.ProtocolName,
		AssignmentBytes: syncGroupRequest.Assignments[0].Assignment, // TODO : handle this properly
	}
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getOffsetFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	offsetFetchRequest := decoder.Decode(&OffsetFetchRequest{}).(*OffsetFetchRequest)
	log.Debug("offsetFetchRequest %+v", offsetFetchRequest)

	response := OffsetFetchResponse{}
	for _, group := range offsetFetchRequest.Groups {
		g := OffsetFetchGroup{GroupID: group.GroupID}

		for _, topic := range group.Topics {
			t := OffsetFetchTopic{Name: topic.Name}
			for _, partitionIndex := range topic.PartitionIndexes {
				t.Partitions = append(t.Partitions, OffsetFetchPartition{
					PartitionIndex:  partitionIndex,
					CommittedOffset: uint64(GetCommittedOffset(g.GroupID, topic.Name, partitionIndex)), // -1 triggers a ListOffsets request
				})
			}
			g.Topics = append(g.Topics, t)
		}
		response.Groups = append(response.Groups, g)
	}
	log.Debug("OffsetFetchResponse %+v", response)

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func getOffsetCommitResponse(req types.Request) []byte {

	decoder := serde.NewDecoder(req.Body)
	offsetCommitRequest := decoder.Decode(&OffsetCommitRequest{}).(*OffsetCommitRequest)
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
	fetchRequest := decoder.Decode(&FetchRequest{}).(*FetchRequest)
	log.Debug("fetchRequest %+v", fetchRequest)

	numTotalRecordBytes := 0
	response := FetchResponse{}
	for _, tp := range fetchRequest.Topics {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp.Name}
		for _, p := range tp.Partitions {
			recordBytes, err := storage.GetRecord(p.FetchOffset, tp.Name, p.PartitionIndex)
			// log.Debug("getFetchResponse GetRecord recordBytes %v", recordBytes)
			numTotalRecordBytes += len(recordBytes)
			if err != nil {
				log.Error("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", uint32(p.FetchOffset), tp, p.PartitionIndex, err)
			}

			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:       p.PartitionIndex,
					HighWatermark:        state.GetPartition(tp.Name, p.PartitionIndex).EndOffset(), //uint64(MinusOne),
					LastStableOffset:     uint64(MinusOne),
					LogStartOffset:       state.GetPartition(tp.Name, p.PartitionIndex).StartOffset(),
					PreferredReadReplica: 1,
					Records:              recordBytes,
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
	listOffsetsRequest := decoder.Decode(&ListOffsetsRequest{}).(*ListOffsetsRequest)
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

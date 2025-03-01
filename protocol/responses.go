package protocol

import (
	"os"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

// ClusterID should be configurable?
var ClusterID = "MONKAFKA-CLUSTER"

// MinusOne used through variable because setting it as uint directly is rejected
var MinusOne int = -1

// DefaultNumPartition represents the num of partitions during creation if unspecified
// TODO: make configurable
const DefaultNumPartition = 1

// InitProducerID (Api key = 22)
func (b *Broker) getInitProducerIDResponse(req types.Request) []byte {
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

func (b *Broker) getJoinGroupResponse(req types.Request) []byte {
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

func (b *Broker) getHeartbeatResponse(req types.Request) []byte {
	response := HeartbeatResponse{}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func (b *Broker) getSyncGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	syncGroupRequest := decoder.Decode(&SyncGroupRequest{}).(*SyncGroupRequest)

	// log.Debug("SyncGroupRequest %+v", syncGroupRequest)
	response := SyncGroupResponse{
		ProtocolType:    syncGroupRequest.ProtocolType,
		ProtocolName:    syncGroupRequest.ProtocolName,
		AssignmentBytes: syncGroupRequest.Assignments[0].Assignment, // TODO : handle this properly
	}
	//  SyncGroupResponseData(throttleTimeMs=0, errorCode=0, protocolType='consumer', protocolName='range', assignment=[0, 3, 0, 0, 0, 1, 0, 4, 116, 105, 116, 105, 0, 0, 0, 1, 0, 0, 0, 0, -1, -1, -1, -1])
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func (b *Broker) getOffsetFetchResponse(req types.Request) []byte {
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

func (b *Broker) getOffsetCommitResponse(req types.Request) []byte {

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

func (b *Broker) getListOffsetsResponse(req types.Request) []byte {
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

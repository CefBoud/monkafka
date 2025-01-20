package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

// Metadata	(Api key = 3)

// MetadataResponseBroker represents a broker in a metadata response.
type MetadataResponseBroker struct {
	NodeID uint32
	Host   string `kafka:"CompactString"`
	Port   uint32
	Rack   string `kafka:"CompactString"`
}

// MetadataResponsePartition represents partition information in a metadata response.
type MetadataResponsePartition struct {
	ErrorCode       uint16
	PartitionIndex  uint32
	LeaderID        uint32
	LeaderEpoch     uint32
	ReplicaNodes    []uint32
	IsrNodes        []uint32
	OfflineReplicas []uint32
}

// MetadataRequest represents a metadata request.
type MetadataRequest struct {
	Topics                           []MetadataRequestTopic
	AllowAutoTopicCreation           bool
	IncludeTopicAuthorizedOperations bool
}

// MetadataRequestTopic represents a topic in the metadata request.
type MetadataRequestTopic struct {
	TopicID [16]byte
	Name    string `kafka:"CompactString"`
}

// MetadataResponseTopic represents a topic in the metadata response.
type MetadataResponseTopic struct {
	ErrorCode                 uint16
	Name                      string `kafka:"CompactString"`
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                []MetadataResponsePartition
	TopicAuthorizedOperations uint32
}

// MetadataResponse represents a metadata response with brokers, topics, and more.
type MetadataResponse struct {
	ThrottleTimeMs uint32
	Brokers        []MetadataResponseBroker
	ClusterID      string `kafka:"CompactString"` // nullable
	ControllerID   uint32
	Topics         []MetadataResponseTopic
}

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

		if state.TopicExists(topic.Name) {
			for partitionIndex := range state.TopicStateInstance[types.TopicName(topic.Name)] {
				topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
					PartitionIndex: uint32(partitionIndex),
					LeaderID:       1,
					ReplicaNodes:   []uint32{1},
					IsrNodes:       []uint32{1}})
			}
		} else {
			if metadataRequest.AllowAutoTopicCreation {
				err := storage.CreateTopic(topic.Name, 1)
				if err != nil {
					log.Error("Error creating topic. %v", err)
				}
			} else {
				topic.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
			}
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

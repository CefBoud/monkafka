package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
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

func (b *Broker) getMetadataResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	metadataRequest := decoder.Decode(&MetadataRequest{}).(*MetadataRequest)
	log.Debug("metadataRequest %+v", metadataRequest)
	// log.Debug("metadataRequest isBroker Controller %+v", b.IsController())
	topics := make([]MetadataResponseTopic, len(metadataRequest.Topics))

	brokers := []MetadataResponseBroker{}

	nodes, err := b.GetClusterNodes()
	if err != nil {
		log.Error("Error get cluster nodes. %v", err)
	}

	controllerID := uint32(MinusOne)
	for i, n := range nodes {
		log.Debug("Metadata Node %v : %+v", i, n)
		brokers = append(brokers, MetadataResponseBroker{
			NodeID: n.NodeID,
			Host:   n.Host,
			Port:   n.Port,
		})
		if n.IsController {
			controllerID = n.NodeID
		}
	}

	foundController := controllerID != uint32(MinusOne)

	// https://github.com/apache/kafka/blob/430892654bcf45d644e66b532d83aab0f569cb7d/clients/src/main/resources/common/message/MetadataRequest.json#L26-L27
	// An empty array indicates "request metadata for no topics," and a null array is used to
	// indicate "request metadata for all topics."
	// TODO: we are not handling the null array case ..
	if len(metadataRequest.Topics) == 0 {
		for t := range b.FSM.Topics {
			metadataRequest.Topics = append(metadataRequest.Topics, MetadataRequestTopic{Name: t})
		}
	}
	for _, topic := range metadataRequest.Topics {
		topic := MetadataResponseTopic{Name: topic.Name, TopicID: topic.TopicID, IsInternal: false}

		if foundController {
			log.Debug("fsm.Topics: %+v", b.FSM.Topics)
			if b.FSM.TopicExists(topic.Name) {
				for partitionIndex, partition := range b.FSM.Topics[topic.Name].Partitions {
					topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
						PartitionIndex: uint32(partitionIndex),
						LeaderID:       partition.LeaderID,
						ReplicaNodes:   []uint32{partition.LeaderID},  // TODO fix this
						IsrNodes:       []uint32{partition.LeaderID}}) // TODO fix this
				}
			} else {
				if metadataRequest.AllowAutoTopicCreation {
					err := b.CreateTopicPartitions(topic.Name, 1, nil)
					if err != nil {
						log.Error("Error creating topic. %v", err)
					}
				} else {
					topic.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				}
			}
		} else {
			topic.ErrorCode = uint16(ErrLeaderNotAvailable.Code)
		}

		topics = append(topics, topic)
	}

	response := MetadataResponse{
		ThrottleTimeMs: 0,
		Brokers:        brokers,
		ClusterID:      ClusterID,
		ControllerID:   controllerID,
		Topics:         topics,
	}

	log.Debug("MetadataResponse %+v", response)
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

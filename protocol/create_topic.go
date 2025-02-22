package protocol

import (
	"fmt"
	"math/rand"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/raft"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/types"
)

// CreateTopicsResponse represents the response to a topic creation request.
type CreateTopicsResponse struct {
	ThrottleTimeMs uint32
	Topics         []CreateTopicsResponseTopic
}

// CreateTopicsResponseTopic represents a topic's creation result.
type CreateTopicsResponseTopic struct {
	Name              string `kafka:"CompactString"`
	TopicID           [16]byte
	ErrorCode         uint16
	ErrorMessage      string `kafka:"CompactString"`
	NumPartitions     uint32
	ReplicationFactor uint16
	Configs           []CreateTopicsResponseConfig
}

// CreateTopicsRequest represents the Kafka request to create topics.
type CreateTopicsRequest struct {
	Topics       []CreateTopicsRequestTopic
	TimeoutMs    uint32
	ValidateOnly bool
}

// CreateTopicsRequestTopic represents the details of a topic to be created.
type CreateTopicsRequestTopic struct {
	Name              string `kafka:"CompactString"`
	NumPartitions     uint32
	ReplicationFactor uint16
	Assignments       []CreateTopicsRequestAssignment
	Configs           []CreateTopicsRequestConfig
}

// CreateTopicsRequestAssignment represents the partition assignments for a topic.
type CreateTopicsRequestAssignment struct {
	PartitionIndex uint32
	BrokerIds      []uint32
}

// CreateTopicsRequestConfig represents the configuration for a topic.
type CreateTopicsRequestConfig struct {
	Name  string `kafka:"CompactString"`
	Value string `kafka:"CompactNullableString"`
}

// CreateTopicsResponseConfig represents a configuration for a topic.
type CreateTopicsResponseConfig struct {
	Name         string
	Value        string
	ReadOnly     bool
	ConfigSource uint8
	IsSensitive  bool
}

// CreateTopics	(Api key = 19)
func (b *Broker) getCreateTopicResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	createTopicsRequest := decoder.Decode(&CreateTopicsRequest{}).(*CreateTopicsRequest)
	log.Debug("CreateTopicsRequest %+v", createTopicsRequest)
	response := CreateTopicsResponse{}

	for _, topic := range createTopicsRequest.Topics {

		if int32(topic.NumPartitions) == -1 {
			topic.NumPartitions = DefaultNumPartition
		}
		topicResponse := CreateTopicsResponseTopic{
			Name:              topic.Name,
			TopicID:           [16]byte{},
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
			Configs:           []CreateTopicsResponseConfig{}, // TODO handle conf
		}

		if !b.IsController() {
			log.Error("getCreateTopicResponse: not controller")
			topicResponse.ErrorCode = uint16(ErrNotController.Code)
			topicResponse.ErrorMessage = ErrNotController.Message
		} else {
			if b.FSM.TopicExists(topic.Name) {
				topicResponse.ErrorCode = uint16(ErrTopicAlreadyExists.Code)
				topicResponse.ErrorMessage = ErrTopicAlreadyExists.Message

			} else {
				configs := make(map[string]string)
				for _, c := range topic.Configs {
					configs[c.Name] = c.Value
				}
				err := b.CreateTopicPartitions(topic.Name, topic.NumPartitions, configs)
				// resp, err := b.AppendRaftEntry(raft.AddTopic, raft.Topic{Name: topic.Name})
				if err != nil {
					log.Error("Error CreateTopicPartitions %v", err)
				}
			}
		}
		response.Topics = append(response.Topics, topicResponse)

	}
	log.Debug("CreateTopicResponse %+v", response)
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

// CreateTopicPartitions creates a new topic with its partition by appending them to the raft log.
func (b *Broker) CreateTopicPartitions(name string, numPartitions uint32, configs map[string]string) error {
	resp, err := b.AppendRaftEntry(raft.AddTopic, types.Topic{Name: name, Configs: configs})
	if err != nil {
		log.Error("Error append topic to raft log %v", err)
	}
	log.Debug("raft AddTopic entry for %v with configs [%+v]. Result: %v ", name, configs, resp)

	nodes, err := b.GetClusterNodes()
	if err != nil || len(nodes) < 1 {
		return fmt.Errorf("CreateTopicPartitions GetClusterNodes error: %v", err)
	}

	for i := uint32(0); i < numPartitions; i++ {
		// pick a leader randomly
		leaderID := nodes[rand.Intn(len(nodes))].NodeID
		partition := types.PartitionState{
			Topic:          name,
			PartitionIndex: i,
			LeaderID:       leaderID,
		}
		log.Debug("adding partition %+v", partition)
		resp, err = b.AppendRaftEntry(raft.AddPartition, partition)
		if err != nil {
			return fmt.Errorf("Error appending partition to raft log %v", err)
		}
		log.Debug("raft AddPartition entry for %v. Result: %v ", name, resp)
	}
	return nil
}

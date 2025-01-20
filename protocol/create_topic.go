package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
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
func getCreateTopicResponse(req types.Request) []byte {
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
		if state.TopicExists(topic.Name) {
			topicResponse.ErrorCode = uint16(ErrTopicAlreadyExists.Code)
			topicResponse.ErrorMessage = ErrTopicAlreadyExists.Message

		} else {
			err := storage.CreateTopic(topic.Name, topic.NumPartitions)
			if err != nil {
				log.Error("Error creating topic %v", err)
			}
		}
		response.Topics = append(response.Topics, topicResponse)

	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

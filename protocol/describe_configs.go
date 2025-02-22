package protocol

import (
	"strconv"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/types"
)

// ResourceType represents the kafka resource type (topic, group, etc.)
type ResourceType int8

// https://github.com/apache/kafka/blob/c6335c2ae86913954d940036917b7556e9ac0460/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java#L31
const (
	ResourceTypeUnknown         ResourceType = 0
	ResourceTypeAny             ResourceType = 1
	ResourceTypeTopic           ResourceType = 2
	ResourceTypeGroup           ResourceType = 3
	ResourceTypeBroker          ResourceType = 4
	ResourceTypeCluster         ResourceType = 4
	ResourceTypeTransactionalID ResourceType = 5
	ResourceTypeDelegationToken ResourceType = 6
)

// DescribeConfigsRequest struct represents the request for DescribeConfigs with version 4.
type DescribeConfigsRequest struct {
	Resources            []DescribeConfigsResource
	IncludeSynonyms      bool
	IncludeDocumentation bool
}

// DescribeConfigsResource struct represents the resource inside DescribeConfigsRequest.
type DescribeConfigsResource struct {
	ResourceType      uint8
	ResourceName      string   `kafka:"CompactString"`
	ConfigurationKeys []string `kafka:"CompactString"`
}

// DescribeConfigsResponse struct represents the response for DescribeConfigs with version 4.
type DescribeConfigsResponse struct {
	ThrottleTimeMs uint32
	Results        []DescribeConfigsResponseResult
}

// DescribeConfigsResponseResult struct represents the result inside DescribeConfigsResponse.
type DescribeConfigsResponseResult struct {
	ErrorCode    uint16
	ErrorMessage string `kafka:"CompactString"`
	ResourceType uint8
	ResourceName string `kafka:"CompactString"`
	Configs      []DescribeConfigsResponseConfig
}

// DescribeConfigsResponseConfig struct represents the configuration details within a result.
type DescribeConfigsResponseConfig struct {
	Name          string
	Value         string `kafka:"CompactNullableString"`
	ReadOnly      bool
	ConfigSource  uint8
	IsSensitive   bool
	Synonyms      []DescribeConfigsResponseSynonym
	ConfigType    uint
	Documentation string `kafka:"CompactNullableString"`
}

// DescribeConfigsResponseSynonym struct represents synonym details within a config.
type DescribeConfigsResponseSynonym struct {
	Name   string `kafka:"CompactString"`
	Value  string `kafka:"CompactNullableString"`
	Source uint
}

// DescribeConfigs	(Api key = 32)
func (b *Broker) getDescribeConfigsResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	describeConfigsRequest := decoder.Decode(&DescribeConfigsRequest{}).(*DescribeConfigsRequest)
	log.Debug("DescribeConfigsRequest %+v", describeConfigsRequest)
	response := DescribeConfigsResponse{}

	for _, resourceConfReq := range describeConfigsRequest.Resources {
		resourceResp := DescribeConfigsResponseResult{
			ResourceType: resourceConfReq.ResourceType,
			ResourceName: resourceConfReq.ResourceName,
		}
		// TODO: handle other resource types
		if ResourceType(resourceConfReq.ResourceType) == ResourceTypeTopic {
			topic, ok := b.FSM.GetTopic(resourceConfReq.ResourceName)
			if !ok {
				resourceResp.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				resourceResp.ErrorMessage = ErrUnknownTopicOrPartition.Message
			} else {
				resourceResp.Configs = append(resourceResp.Configs, DescribeConfigsResponseConfig{
					Name:  "partitions",
					Value: strconv.Itoa(len(topic.Partitions)),
				})
				for key, value := range topic.Configs {
					resourceResp.Configs = append(resourceResp.Configs, DescribeConfigsResponseConfig{
						Name:  key,
						Value: value,
					})
				}
			}
			response.Results = append(response.Results, resourceResp)
		}
	}
	log.Debug("DescribeConfigsResponse %+v", response)
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

package protocol

import "github.com/CefBoud/monkafka/types"

// https://kafka.apache.org/protocol#protocol_api_keys
var produceKey = uint16(0)
var fetchKey = uint16(1)
var listOffsetsKey = uint16(2)
var metadataKey = uint16(3)
var offsetCommitKey = uint16(8)
var offsetFetchKey = uint16(9)
var findCoordinatorKey = uint16(10)
var joinGroupKey = uint16(11)
var heartbeatKey = uint16(12)
var syncGroupKey = uint16(14)
var apiVersionKey = uint16(18)
var createTopicKey = uint16(19)
var initProducerIDKey = uint16(22)
var describeConfigsKey = uint16(32)

// APIKeyHandler represents a kafka api key with its handler
type APIKeyHandler struct {
	Name    string
	Handler func(req types.Request) []byte
}

// APIDispatcher maps the Request key to its handler
func (b *Broker) APIDispatcher(requestAPIKey uint16) APIKeyHandler {
	switch requestAPIKey {
	case produceKey:
		return APIKeyHandler{Name: "Produce", Handler: b.getProduceResponse}
	case fetchKey:
		return APIKeyHandler{Name: "Fetch", Handler: b.getFetchResponse}
	case listOffsetsKey:
		return APIKeyHandler{Name: "ListOffsets", Handler: b.getListOffsetsResponse}
	case metadataKey:
		return APIKeyHandler{Name: "Metadata", Handler: b.getMetadataResponse}
	case offsetCommitKey:
		return APIKeyHandler{Name: "OffsetCommit", Handler: b.getOffsetCommitResponse}
	case offsetFetchKey:
		return APIKeyHandler{Name: "OffsetFetch", Handler: b.getOffsetFetchResponse}
	case findCoordinatorKey:
		return APIKeyHandler{Name: "FindCoordinator", Handler: b.getFindCoordinatorResponse}
	case joinGroupKey:
		return APIKeyHandler{Name: "JoinGroup", Handler: b.getJoinGroupResponse}
	case heartbeatKey:
		return APIKeyHandler{Name: "Heartbeat", Handler: b.getHeartbeatResponse}
	case syncGroupKey:
		return APIKeyHandler{Name: "SyncGroup", Handler: b.getSyncGroupResponse}
	case apiVersionKey:
		return APIKeyHandler{Name: "APIVersion", Handler: b.getAPIVersionResponse}
	case createTopicKey:
		return APIKeyHandler{Name: "CreateTopic", Handler: b.getCreateTopicResponse}
	case initProducerIDKey:
		return APIKeyHandler{Name: "InitProducerID", Handler: b.getInitProducerIDResponse}
	case describeConfigsKey:
		return APIKeyHandler{Name: "DescribeConfigs", Handler: b.getDescribeConfigsResponse}
	default:
		return APIKeyHandler{}
	}

}

// ConsumerOffsetsTopic is the topic where consumers committed offsets are saved
var ConsumerOffsetsTopic = "__consumer-offsets"

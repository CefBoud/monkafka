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

// APIDispatcher maps the Request key to its handler
var APIDispatcher = map[uint16]struct {
	Name    string
	Handler func(req types.Request) []byte
}{
	produceKey:         {Name: "Produce", Handler: getProduceResponse},
	fetchKey:           {Name: "Fetch", Handler: getFetchResponse},
	listOffsetsKey:     {Name: "ListOffsets", Handler: getListOffsetsResponse},
	metadataKey:        {Name: "Metadata", Handler: getMetadataResponse},
	offsetCommitKey:    {Name: "OffsetCommit", Handler: getOffsetCommitResponse},
	offsetFetchKey:     {Name: "OffsetFetch", Handler: getOffsetFetchResponse},
	findCoordinatorKey: {Name: "FindCoordinator", Handler: getFindCoordinatorResponse},
	joinGroupKey:       {Name: "JoinGroup", Handler: getJoinGroupResponse},
	heartbeatKey:       {Name: "Heartbeat", Handler: getHeartbeatResponse},
	syncGroupKey:       {Name: "SyncGroup", Handler: getSyncGroupResponse},
	apiVersionKey:      {Name: "APIVersion", Handler: getAPIVersionResponse},
	createTopicKey:     {Name: "CreateTopic", Handler: getCreateTopicResponse},
	initProducerIDKey:  {Name: "InitProducerID", Handler: getInitProducerIDResponse},
}

// ConsumerOffsetsTopic is the topic where consumers committed offsets are saved
var ConsumerOffsetsTopic = "__consumer-offsets"

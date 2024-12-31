package protocol

import "github.com/CefBoud/monkafka/types"

// https://kafka.apache.org/protocol#protocol_api_keys
var ProduceKey = uint16(0)
var FetchKey = uint16(1)
var ListOffsetsKey = uint16(2)
var MetadataKey = uint16(3)
var OffsetCommitKey = uint16(8)
var OffsetFetchKey = uint16(9)
var FindCoordinatorKey = uint16(10)
var JoinGroupKey = uint16(11)
var HeartbeatKey = uint16(12)
var SyncGroupKey = uint16(14)
var APIVersionKey = uint16(18)
var CreateTopicKey = uint16(19)
var InitProducerIdKey = uint16(22)

var APIDispatcher = map[uint16]struct {
	Name    string
	Handler func(req types.Request) []byte
}{
	ProduceKey:         {Name: "Produce", Handler: getProduceResponse},
	FetchKey:           {Name: "Fetch", Handler: getFetchResponse},
	ListOffsetsKey:     {Name: "ListOffsets", Handler: getListOffsetsResponse},
	MetadataKey:        {Name: "Metadata", Handler: getMetadataResponse},
	OffsetCommitKey:    {Name: "OffsetCommit", Handler: getOffsetCommitResponse},
	OffsetFetchKey:     {Name: "OffsetFetch", Handler: getOffsetFetchResponse},
	FindCoordinatorKey: {Name: "FindCoordinator", Handler: getFindCoordinatorResponse},
	JoinGroupKey:       {Name: "JoinGroup", Handler: getJoinGroupResponse},
	HeartbeatKey:       {Name: "Heartbeat", Handler: getHeartbeatResponse},
	SyncGroupKey:       {Name: "SyncGroup", Handler: getSyncGroupResponse},
	APIVersionKey:      {Name: "APIVersion", Handler: getAPIVersionResponse},
	CreateTopicKey:     {Name: "CreateTopic", Handler: getCreateTopicResponse},
	InitProducerIdKey:  {Name: "InitProducerId", Handler: getInitProducerIdResponse},
}

var ConsumerOffsetsTopic = "__consumer-offsets"

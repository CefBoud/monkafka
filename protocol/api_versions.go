package protocol

import (
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/types"
)

// APIKey represents an API key and its supported version range.
type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

// APIVersionsResponse represents the response for API versions request.
type APIVersionsResponse struct {
	ErrorCode    uint16
	APIKeys      []APIKey
	ThrottleTime uint32
}

// APIVersion (Api key = 18)
func (b *Broker) getAPIVersionResponse(req types.Request) []byte {
	response := APIVersionsResponse{
		APIKeys: []APIKey{
			{APIKey: produceKey, MinVersion: 0, MaxVersion: 11},
			{APIKey: fetchKey, MinVersion: 12, MaxVersion: 12},
			{APIKey: listOffsetsKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: metadataKey, MinVersion: 0, MaxVersion: 12},
			{APIKey: offsetCommitKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: offsetFetchKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: findCoordinatorKey, MinVersion: 3, MaxVersion: 6},
			{APIKey: joinGroupKey, MinVersion: 0, MaxVersion: 9},
			{APIKey: heartbeatKey, MinVersion: 0, MaxVersion: 4},
			{APIKey: syncGroupKey, MinVersion: 0, MaxVersion: 5},
			{APIKey: apiVersionKey, MinVersion: 0, MaxVersion: 4},
			{APIKey: createTopicKey, MinVersion: 0, MaxVersion: 7},
			{APIKey: initProducerIDKey, MinVersion: 0, MaxVersion: 5},
			{APIKey: describeConfigsKey, MinVersion: 0, MaxVersion: 4},
		},
	}

	// we are not using EncodeResponseBytes because APIversion doesn't use tagged buffers
	encoder := serde.NewEncoder()
	encoder.PutInt32(req.CorrelationID)
	encoder.Encode(response)
	encoder.PutLen()
	return encoder.Bytes()
}

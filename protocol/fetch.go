package protocol

import (
	"time"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

// FetchRequest represents the details of a FetchRequest (Version: 12).
type FetchRequest struct {
	ReplicaID           uint32
	MaxWaitMs           uint32
	MinBytes            uint32
	MaxBytes            uint32
	IsolationLevel      uint8
	SessionID           uint32
	SessionEpoch        uint32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopic
	RackID              string `kafka:"CompactString"`
}

// FetchRequestTopic represents the topic-level data in a FetchRequest.
type FetchRequestTopic struct {
	Name       string `kafka:"CompactString"`
	Partitions []FetchRequestPartitionData
}

// FetchRequestPartitionData represents the partition-level data in a FetchRequest.
type FetchRequestPartitionData struct {
	PartitionIndex     uint32
	CurrentLeaderEpoch uint32
	FetchOffset        uint64
	LastFetchedEpoch   uint32
	LogStartOffset     uint64
	PartitionMaxBytes  uint32
}

// FetchRequestForgottenTopic represents the forgotten topic data in a FetchRequest.
type FetchRequestForgottenTopic struct {
	Topic      string `kafka:"CompactString"`
	Partitions []uint32
}

// FetchResponse represents the response to a fetch request.
type FetchResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionID      uint32
	Responses      []FetchTopicResponse
}

// FetchTopicResponse represents the response for a topic in a fetch request.
type FetchTopicResponse struct {
	TopicName  string `kafka:"CompactString"`
	Partitions []FetchPartitionResponse
}

// FetchPartitionResponse represents the response for a partition in a fetch request.
type FetchPartitionResponse struct {
	PartitionIndex       uint32
	ErrorCode            uint16
	HighWatermark        uint64
	LastStableOffset     uint64
	LogStartOffset       uint64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica uint32
	Records              []byte
}

// AbortedTransaction represents an aborted transaction in the fetch response.
type AbortedTransaction struct {
	ProducerID  uint64
	FirstOffset uint64
}

func getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	fetchRequest := decoder.Decode(&FetchRequest{}).(*FetchRequest)
	log.Debug("fetchRequest %+v", fetchRequest)

	numTotalRecordBytes := 0
	response := FetchResponse{}
	for _, tp := range fetchRequest.Topics {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp.Name}
		for _, p := range tp.Partitions {
			recordBytes, err := storage.GetRecordBatch(p.FetchOffset, tp.Name, p.PartitionIndex)
			// log.Debug("getFetchResponse GetRecord recordBytes %v", recordBytes)
			numTotalRecordBytes += len(recordBytes)
			if err != nil {
				log.Error("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", uint32(p.FetchOffset), tp, p.PartitionIndex, err)
			}

			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:       p.PartitionIndex,
					HighWatermark:        state.GetPartition(tp.Name, p.PartitionIndex).EndOffset(), //uint64(MinusOne),
					LastStableOffset:     uint64(MinusOne),
					LogStartOffset:       state.GetPartition(tp.Name, p.PartitionIndex).StartOffset(),
					PreferredReadReplica: 1,
					Records:              recordBytes,
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	if numTotalRecordBytes == 0 {
		log.Info("There is no data available for this fetch request, waiting for a bit ..")
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

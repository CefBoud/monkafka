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

func decodeFetchRequest(d serde.Decoder, fetchRequest *FetchRequest) {

	fetchRequest.ReplicaID = d.UInt32()
	fetchRequest.MaxWaitMs = d.UInt32()
	fetchRequest.MinBytes = d.UInt32()
	fetchRequest.MaxBytes = d.UInt32()
	fetchRequest.IsolationLevel = d.UInt8()
	fetchRequest.SessionID = d.UInt32()
	fetchRequest.SessionEpoch = d.UInt32()

	lenTopic := int(d.CompactArrayLen())

	for i := 0; i < lenTopic; i++ {
		topic := FetchRequestTopic{Name: d.CompactString()}
		lenPartitions := int(d.CompactArrayLen())
		for j := 0; j < lenPartitions; j++ {
			topic.Partitions = append(topic.Partitions, FetchRequestPartitionData{
				PartitionIndex:     d.UInt32(),
				CurrentLeaderEpoch: d.UInt32(),
				FetchOffset:        d.UInt64(),
				LastFetchedEpoch:   d.UInt32(),
				LogStartOffset:     d.UInt64(),
				PartitionMaxBytes:  d.UInt32(),
			})
			d.EndStruct()
		}
		fetchRequest.Topics = append(fetchRequest.Topics, topic)
		d.EndStruct()
	}
	return
}

func (b *Broker) getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	fetchRequest := &FetchRequest{}
	// for perf sensitive requests, we don't rely on reflection
	decodeFetchRequest(decoder, fetchRequest)
	log.Debug("fetchRequest %+v", fetchRequest)

	numTotalRecordBytes := 0
	response := FetchResponse{}
	for _, tp := range fetchRequest.Topics {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp.Name}
		for _, p := range tp.Partitions {
			partition, exists := b.FSM.GetPartition(tp.Name, p.PartitionIndex)
			if !exists {
				response.ErrorCode = uint16(ErrReplicaNotAvailable.Code)
				goto FINISH
			}
			if partition.LeaderID != b.FSM.NodeID {
				log.Error("Fetch: Not leader for partition %v-%v", partition.Topic, partition.PartitionIndex)
				response.ErrorCode = uint16(ErrNotLeaderOrFollower.Code)
				goto FINISH
			}
			recordBytes, err := storage.GetRecordBatch(p.FetchOffset, tp.Name, p.PartitionIndex)
			// log.Debug("getFetchResponse GetRecord recordBytes %v", recordBytes)
			numTotalRecordBytes += len(recordBytes)
			if err != nil {
				log.Error("Error while fetching record at currentOffset:%v  for topic %v-%v | err: %v", uint32(p.FetchOffset), tp, p.PartitionIndex, err)
			}

			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:   p.PartitionIndex,
					HighWatermark:    state.GetPartition(tp.Name, p.PartitionIndex).EndOffset(), //uint64(MinusOne),
					LastStableOffset: uint64(MinusOne),
					LogStartOffset:   state.GetPartition(tp.Name, p.PartitionIndex).StartOffset(),
					// PreferredReadReplica: 1,
					Records: recordBytes,
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	if numTotalRecordBytes == 0 {
		log.Info("There is no data available for this fetch request, waiting for a bit ..")
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	}
FINISH:
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

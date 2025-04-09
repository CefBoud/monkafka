package protocol

import (
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

// ProduceRequest represents the details of a ProduceRequest.
type ProduceRequest struct {
	TransactionalID string `kafka:"CompactNullableString"`
	Acks            uint16
	TimeoutMs       uint32
	TopicData       []ProduceRequestTopicData
}

// ProduceRequestTopicData represents the topic data in a ProduceRequest.
type ProduceRequestTopicData struct {
	Name          string `kafka:"CompactString"`
	PartitionData []ProduceRequestPartitionData
}

// ProduceRequestPartitionData represents the partition data in a ProduceRequest.
type ProduceRequestPartitionData struct {
	Index   uint32
	Records []byte
}

// ProduceResponse represents the response to a produce request.
type ProduceResponse struct {
	ProduceTopicResponses []ProduceTopicResponse
	ThrottleTimeMs        uint32
}

// ProduceTopicResponse represents the response for a topic in a produce request.
type ProduceTopicResponse struct {
	Name                      string `kafka:"CompactString"`
	ProducePartitionResponses []ProducePartitionResponse
}

// ProducePartitionResponse represents the response for a partition in a produce request.
type ProducePartitionResponse struct {
	Index           uint32
	ErrorCode       uint16
	BaseOffset      uint64
	LogAppendTimeMs uint64
	LogStartOffset  uint64
	RecordErrors    []RecordError
	ErrorMessage    string `kafka:"CompactString"`
}

// RecordError represents an error in a specific batch of records.
type RecordError struct {
	BatchIndex             uint32
	BatchIndexErrorMessage string // compact_nullable
}

func decodeProduceRequest(d serde.Decoder, produceRequest *ProduceRequest) {
	produceRequest.TransactionalID = d.CompactString()
	produceRequest.Acks = d.UInt16()
	produceRequest.TimeoutMs = d.UInt32()
	lenTopicData := int(d.CompactArrayLen())
	for i := 0; i < lenTopicData; i++ {
		topic := ProduceRequestTopicData{Name: d.CompactString()}
		lenPartitionData := int(d.CompactArrayLen())
		for j := 0; j < lenPartitionData; j++ {
			topic.PartitionData = append(topic.PartitionData, ProduceRequestPartitionData{
				Index: d.UInt32(), Records: d.CompactBytes(),
			})
			d.EndStruct()
		}
		produceRequest.TopicData = append(produceRequest.TopicData, topic)
		d.EndStruct()
	}
	return
}

func (b *Broker) getProduceResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	produceRequest := &ProduceRequest{}
	// for perf sensitive requests, we don't rely on reflection
	decodeProduceRequest(decoder, produceRequest)
	log.Debug("ProduceRequest %+v", produceRequest)
	response := ProduceResponse{}

	for _, td := range produceRequest.TopicData {
		produceTopicResponse := ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.PartitionData {
			partitionResponse := ProducePartitionResponse{
				Index:      pd.Index,
				BaseOffset: 0} // TODO: should this be 0? probably not..
			if state.PartitionExists(td.Name, pd.Index) {
				err := storage.AppendRecord(td.Name, pd.Index, pd.Records)
				if err != nil {
					log.Error("Error AppendRecord: %v", err)
				} else {
					partitionResponse.LogAppendTimeMs = utils.NowAsUnixMilli()
				}
			} else {
				partitionResponse.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				partitionResponse.ErrorMessage = ErrUnknownTopicOrPartition.Message
			}

			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, partitionResponse)
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

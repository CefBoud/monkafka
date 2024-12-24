package types

type Request struct {
	Length            uint32
	RequestApiKey     uint16
	RequestApiVersion uint16
	CorrelationID     uint32
	ClientId          string
	ConnectionAddress string
	Buffer            []byte
}

type Offset uint32
type PartitionIndex uint32
type TopicName string
type TopicsState map[TopicName]map[PartitionIndex]Offset

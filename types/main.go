package types

import (
	"os"
	"sync"
)

type Request struct {
	Length            uint32
	RequestApiKey     uint16
	RequestApiVersion uint16
	CorrelationID     uint32
	ClientId          string
	ConnectionAddress string
	Body              []byte
}

type PartitionIndex uint32
type TopicName string
type Partition struct {
	LastOffset         uint32
	NextRecordPosition uint32
	IndexFile          *os.File
	IndexData          []byte // TODO: use mmap?
	SegmentFile        *os.File
	sync.RWMutex
}
type TopicsState map[TopicName]map[PartitionIndex]*Partition

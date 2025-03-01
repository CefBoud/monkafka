package types

import (
	"fmt"
	"os"
	"sync"
)

// PartitionIndex is the type representing the partition index of a Kafka topic.
type PartitionIndex uint32

// TopicName is the type representing the name of a Kafka topic.
type TopicName string

// Record represents a single Kafka message or record, which can include optional headers, key, and value.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []Header
}

// Header represents a single header in a Kafka record, consisting of a key and its associated value.
type Header struct {
	HeaderKeyLength   int
	HeaderKey         string
	HeaderValueLength int
	Value             []byte
}

// RecordBatch represents a batch of records in a Kafka partition, with metadata such as offsets, timestamps, and producer information.
type RecordBatch struct {
	BaseOffset           uint64
	BatchLength          uint32
	PartitionLeaderEpoch uint32
	Magic                uint8
	CRC                  uint32
	Attributes           uint16
	LastOffsetDelta      uint32 // delta added to BaseOffset to get the Batch's last offset
	BaseTimestamp        uint64
	MaxTimestamp         uint64
	ProducerID           uint64
	ProducerEpoch        uint16
	BaseSequence         uint32
	NumRecord            uint32
	Records              []byte
}

// Segment represents a segment of a Kafka partition, consisting of log and index files along with metadata about the segment.
type Segment struct {
	LogFile      *os.File
	IndexFile    *os.File
	IndexData    []byte
	StartOffset  uint64
	EndOffset    uint64
	LogFileSize  uint32
	MaxTimestamp uint64
	sync.RWMutex
}

// Partition represents a Kafka partition, which includes topic information, segments, and methods for accessing partition offsets.
type Partition struct {
	TopicName string
	Index     uint32
	Segments  []*Segment
	sync.RWMutex
}

// String provides a string representation of the partition, combining the topic name and partition index.
func (p *Partition) String() string {
	return fmt.Sprintf("%v-%v", p.TopicName, p.Index)
}

// ActiveSegment returns the most recent segment in the partition.
func (p *Partition) ActiveSegment() *Segment {
	if len(p.Segments) > 0 {
		return p.Segments[len(p.Segments)-1]
	}
	return nil
}

// StartOffset returns the start offset of the first segment in the partition.
func (p *Partition) StartOffset() uint64 {
	if len(p.Segments) > 0 {
		return p.Segments[0].StartOffset
	}
	return 0
}

// EndOffset returns the end offset of the partition. If the active segment is empty, it may adjust the end offset.
func (p *Partition) EndOffset() uint64 {
	if len(p.Segments) > 0 {
		if p.ActiveSegment().LogFileSize > 0 {
			return p.ActiveSegment().EndOffset
		} else if len(p.Segments) > 1 {
			// If the last segment is empty but the partition isn't, return the last offset minus 1.
			return p.ActiveSegment().EndOffset - 1
		}
	}
	return 0
}

// IsEmpty returns true if the partition has no records or segments.
func (p *Partition) IsEmpty() bool {
	return p.ActiveSegment() == nil || (p.ActiveSegment().LogFileSize == 0 && p.StartOffset() == p.EndOffset())
}

// TopicsState represents the state of all topics and their partitions.
type TopicsState map[TopicName]map[PartitionIndex]*Partition

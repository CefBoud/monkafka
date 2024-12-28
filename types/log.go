package types

import (
	"os"
	"sync"
)

type PartitionIndex uint32
type TopicName string

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
	ProducerId           uint64
	ProducerEpoch        uint16
	BaseSequence         uint32
	Records              []byte //[]Record
}
type Segment struct {
	LogFile      *os.File
	IndexFile    *os.File
	IndexData    []byte // TODO: use mmap?
	StartOffset  uint64
	EndOffset    uint64
	LogFileSize  uint32
	MaxTimestamp uint64 // latest message timestamp
	sync.RWMutex
}
type Partition struct {
	TopicName string
	Index     uint32
	Segments  []*Segment
	sync.RWMutex
}

func (p *Partition) ActiveSegment() *Segment {
	return p.Segments[len(p.Segments)-1]
}
func (p *Partition) StartOffset() uint64 {
	if len(p.Segments) > 0 {
		return p.Segments[0].StartOffset
	}
	return 0
}
func (p *Partition) EndOffset() uint64 {
	if len(p.Segments) > 0 {
		if p.ActiveSegment().LogFileSize > 0 {
			return p.ActiveSegment().EndOffset
		} else if len(p.Segments) > 1 {
			// last segment's empty but partition is not
			return p.ActiveSegment().EndOffset - 1
		}
	}
	return 0
}

func (p *Partition) IsEmpty() bool {
	return p.StartOffset() == p.EndOffset()
}

type TopicsState map[TopicName]map[PartitionIndex]*Partition

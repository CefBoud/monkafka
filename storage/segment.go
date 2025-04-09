package storage

import (
	"fmt"
	"io"

	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/types"
)

const logSuffix = ".log"
const indexSuffix = ".index"

func getLogFile(topic string, partition uint32, baseOffset uint64) string {

	return filepath.Join(GetPartitionDir(topic, partition), fmt.Sprintf("%020d", baseOffset)+".log")
}
func getIndexFile(topic string, partition uint32, baseOffset uint64) string {
	return filepath.Join(GetPartitionDir(topic, partition), fmt.Sprintf("%020d", baseOffset)+".index")
}

// NewSegment create a new segment for the given partition
func NewSegment(p *types.Partition) (*types.Segment, error) {
	var seg *types.Segment
	segmentStartOffset := p.EndOffset()
	if segmentStartOffset > 0 { // if partition's EndOffset == 0, no need to ++
		segmentStartOffset++
	}
	indexFile, err := os.OpenFile(getIndexFile(p.TopicName, p.Index, segmentStartOffset), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Error("Error creating index file: %v", err)
		return seg, err
	}
	logFile, err := os.OpenFile(getLogFile(p.TopicName, p.Index, segmentStartOffset), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Error("Error creating segment file: %v", err)
		return seg, err
	}

	seg = &types.Segment{
		LogFile:     logFile,
		IndexFile:   indexFile,
		StartOffset: segmentStartOffset,
		EndOffset:   segmentStartOffset,
	}

	return seg, nil
}

// LoadSegments loads segments with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
func LoadSegments(partitionDir string) ([]*types.Segment, error) {
	var segments []*types.Segment
	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), logSuffix) {
			logFilePath := filepath.Join(partitionDir, entry.Name())
			logFile, err := os.OpenFile(logFilePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, fmt.Errorf("error opening logFile %v. %v", filepath.Join(partitionDir, entry.Name()), err)
			}
			indexFilePath := strings.Replace(logFilePath, logSuffix, indexSuffix, 1)
			indexFile, err := os.OpenFile(indexFilePath, os.O_RDWR, 0644)

			if err != nil {
				return nil, fmt.Errorf("error opening logFile %v. %v", filepath.Join(partitionDir, entry.Name()), err)
			}
			indexData, err := io.ReadAll(indexFile)
			if err != nil {
				return nil, err
			}

			stat, err := logFile.Stat()
			if err != nil {
				return nil, fmt.Errorf("error reading log file info: %v", err)
			}
			logFileSize := uint32(stat.Size())

			startOffset, err := strconv.ParseUint(strings.Replace(entry.Name(), logSuffix, "", 1), 10, 64)
			if err != nil {
				return nil, err
			}
			endOffset := startOffset
			var lastRecordBatch types.RecordBatch
			if len(indexData) > 0 {
				lastRecordOffset := serde.Encoding.Uint32(indexData[len(indexData)-8:])
				recordPosition := serde.Encoding.Uint32(indexData[len(indexData)-4:])
				lastBatchRecordBytes := make([]byte, logFileSize-recordPosition)
				_, err := logFile.ReadAt(lastBatchRecordBytes, int64(recordPosition))
				if err != nil {
					return nil, fmt.Errorf("error while reading %v. %v ", logFilePath, err)
				}
				lastRecordBatch = ReadRecordBatch(lastBatchRecordBytes)
				// offset written in the index are relative to the start, hence endOffset += not endOffset =
				endOffset += uint64(lastRecordOffset + lastRecordBatch.LastOffsetDelta)
			}

			log.Info("loading segment  %v EndOffset %v indexData len %v\n ", indexFilePath, endOffset, len(indexData))
			segments = append(segments, &types.Segment{
				LogFile:      logFile,
				IndexFile:    indexFile,
				IndexData:    indexData,
				StartOffset:  startOffset,
				EndOffset:    endOffset,
				LogFileSize:  logFileSize,
				MaxTimestamp: lastRecordBatch.MaxTimestamp,
			})
		}
	}
	return segments, nil
}

func shouldRollSegment(segment *types.Segment) bool {
	// roll if larger than LogSegmentSizeBytes or older than LogSegmentMs
	return segment.LogFileSize >= uint32(state.Config.LogSegmentSizeBytes) ||
		(segment.MaxTimestamp > 0 && segment.MaxTimestamp < uint64(time.Now().UnixMilli())-state.Config.LogSegmentMs)
}

func shouldDeleteOldestSegment(partition *types.Partition) bool {
	segment := partition.Segments[0]
	return segment.MaxTimestamp > 0 && segment.MaxTimestamp < uint64(time.Now().UnixMilli())-state.Config.LogRetentionMs
}

func rollPartitionSegment(partition *types.Partition) error {
	activeSegment := partition.ActiveSegment()
	activeSegment.Lock()
	defer activeSegment.Unlock()
	seg, err := NewSegment(partition)
	if err != nil {
		return fmt.Errorf("error while creating new segment for roll %v", err)
	}
	partition.Segments = append(partition.Segments, seg)
	return nil
}

func deleteOldestSegment(partition *types.Partition) error {
	segment := partition.Segments[0]
	segment.Lock()
	defer segment.Unlock()
	err := os.Remove(segment.IndexFile.Name())
	if err != nil {
		return fmt.Errorf("failed to move %v with error: %v", segment.IndexFile.Name(), err)
	}
	err = os.Remove(segment.LogFile.Name())
	if err != nil {
		return fmt.Errorf("failed to move %v with error: %v", segment.LogFile.Name(), err)
	}
	partition.Segments = partition.Segments[1:]

	return nil
}

func cleanupPartition(partition *types.Partition) error {
	partition.Lock()
	defer partition.Unlock()

	activeSegment := partition.ActiveSegment()
	if activeSegment != nil {
		if shouldRollSegment(activeSegment) {
			log.Info("rolling segment for partition %v-%v \n", partition.TopicName, partition.Index)
			err := rollPartitionSegment(partition)
			if err != nil {
				return fmt.Errorf("error while rolling partition %v-%v segment %v", partition.TopicName, partition.Index, err)
			}
		}
	} else {
		log.Error("cleanupPartition: partition %v-%v has no active segment", partition.TopicName, partition.Index)
	}

	if len(partition.Segments) > 1 {
		if shouldDeleteOldestSegment(partition) {
			log.Info("deleting oldest segment for partition %v-%v \n", partition.TopicName, partition.Index)
			err := deleteOldestSegment(partition)
			if err != nil {
				return fmt.Errorf("error while deleting oldest segment for partition %v-%v Error: %v", partition.TopicName, partition.Index, err)
			}
		}
	}
	return nil
}

// CleanupSegments takes care of segment retention based on time and size
func CleanupSegments() error {
	log.Info("Running CleanupSegments")
	for _, partitionMap := range state.TopicStateInstance {
		for _, partition := range partitionMap {
			if err := cleanupPartition(partition); err != nil {
				return fmt.Errorf("error while cleanupPartition %v-%v Error: %v", partition.TopicName, partition.Index, err)
			}
		}
	}
	return nil
}

package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
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

// LoadTopicsState loads the state of all topics from disk.
func LoadTopicsState() (types.TopicsState, error) {
	logDir := state.Config.LogDir
	log.Info("Starting with LogDir: %v\n", logDir)
	err := os.MkdirAll(logDir, 0750)
	if err != nil {
		return state.TopicStateInstance, err
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return state.TopicStateInstance, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			lastIndex := strings.LastIndex(entry.Name(), "-")
			if lastIndex == -1 {
				continue
			}
			topicName := entry.Name()[:lastIndex]
			index, err := strconv.Atoi(entry.Name()[lastIndex+1:])
			if err != nil {
				log.Error("Error partition index to int: %v", err)
				return state.TopicStateInstance, err
			}
			if state.TopicStateInstance[types.TopicName(topicName)] == nil {
				state.TopicStateInstance[types.TopicName(topicName)] = make(map[types.PartitionIndex]*types.Partition)
			}

			segments, err := LoadSegments(filepath.Join(logDir, entry.Name()))
			log.Info("loaded %v segments for %v\n", len(segments), entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to load segments from %v. %v", filepath.Join(logDir, entry.Name()), err)
			}
			state.TopicStateInstance[types.TopicName(topicName)][types.PartitionIndex(index)] = &types.Partition{Segments: segments, Index: uint32(index), TopicName: topicName}
		}
	}
	log.Info("loadTopicsState %v", state.TopicStateInstance)
	return state.TopicStateInstance, err
}

// AppendRecord appends a record to the active segment of the specified partition.
func AppendRecord(topic string, partition uint32, recordBytes []byte) error {
	partitionState := state.GetPartition(topic, partition)
	partitionState.Lock()
	defer partitionState.Unlock()
	activeSegment := partitionState.ActiveSegment()

	// Append to the end
	activeSegment.LogFile.Seek(0, io.SeekEnd)
	activeSegment.IndexFile.Seek(0, io.SeekEnd)

	var newOffset = activeSegment.EndOffset
	if len(activeSegment.IndexData) > 0 {
		newOffset = activeSegment.EndOffset + 1
	}

	serde.Encoding.PutUint64(recordBytes, uint64(newOffset))

	n, err := activeSegment.LogFile.Write(recordBytes)
	if n != len(recordBytes) || err != nil {
		log.Error("Error appending record to LogFile for topic %v\n", topic)
	}

	indexEntry := make([]byte, 8)
	serde.Encoding.PutUint32(indexEntry, uint32(newOffset-activeSegment.StartOffset)) // relative to start
	serde.Encoding.PutUint32(indexEntry[4:], activeSegment.LogFileSize)
	activeSegment.IndexFile.Write(indexEntry)

	// Update index
	newIndexData := make([]byte, len(activeSegment.IndexData)+8)
	copy(newIndexData, activeSegment.IndexData)
	copy(newIndexData[len(activeSegment.IndexData):], indexEntry)
	activeSegment.IndexData = newIndexData
	recordBatch := ReadRecordBatch(recordBytes)
	activeSegment.EndOffset = newOffset + uint64(recordBatch.LastOffsetDelta) // Update end offset
	activeSegment.LogFileSize += uint32(len(recordBytes))
	activeSegment.MaxTimestamp = recordBatch.MaxTimestamp
	log.Debug(" newOffset: %v | NextRecordPosition: %v \n ", newOffset, activeSegment.LogFileSize)
	return nil
}

// getClosestIndexEntryIndex finds the closest index entry for the given offset using binary search.
func getClosestIndexEntryIndex(offset uint32, indexData []byte) int {
	lastEntry := len(indexData)/8 - 1
	left, right := 0, lastEntry
	var mid int
	var closestOffset uint32
	for left <= right {
		mid = (left + right) / 2
		closestOffset = serde.Encoding.Uint32(indexData[mid*8:])
		if closestOffset == offset {
			return mid
		} else if closestOffset > offset {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	// in case offset is greater than the latest one, left will exceed lastEntry
	if left >= lastEntry {
		return lastEntry
	}
	// Return the closest greater index
	return left
}

// getOffsetSegment finds the segment containing the given offset.
func getOffsetSegment(offset uint64, partition *types.Partition) (*types.Segment, error) {
	if offset < partition.StartOffset() || offset > partition.EndOffset()+1 {
		return nil, fmt.Errorf("out of range of offset")
	}
	if offset == partition.EndOffset()+1 { // Consumer caught up
		if partition.ActiveSegment().LogFileSize > 0 {
			return partition.ActiveSegment(), nil
		}
		// Return previous segment if active segment is empty
		segmentBeforeActive := partition.Segments[len(partition.Segments)-2]
		return segmentBeforeActive, nil

	}

	// Find the correct segment by offset range
	for i, segment := range partition.Segments {
		if offset >= segment.StartOffset && offset <= segment.EndOffset {
			log.Debug("offset %v is within segment %v bounds [ %v, %v ]", offset, i, segment.StartOffset, segment.EndOffset)
			return segment, nil
		}
	}
	log.Error("mismatch between segments and partition offsets")
	os.Exit(1)
	return nil, nil
}

// GetRecordBatch retrieves the RecordBatch bytes corresponding to the specified offset in the partition.
func GetRecordBatch(offset uint64, topic string, partition uint32) ([]byte, error) {
	log.Debug("GetRecord offset: %v | topic: %v \n\n", offset, topic)
	partitionState := state.GetPartition(topic, partition)
	partitionState.RLock()
	defer partitionState.RUnlock()

	if partitionState.IsEmpty() {
		log.Debug("GetRecord offset: %v | topic: %v | Empty partition \n\n", offset, topic)
		return nil, nil
	}
	targetSegment, err := getOffsetSegment(offset, partitionState)
	if err != nil {
		return nil, fmt.Errorf("error while getting offset segment: %v", err)
	}
	targetSegment.RLock()
	defer targetSegment.RUnlock()

	indexData := targetSegment.IndexData
	var recordStartPosition, recordEndPosition uint32
	// If offset is greater than latest, return latest record
	if len(indexData)/8 > 0 && offset > targetSegment.EndOffset {
		recordStartPosition = serde.Encoding.Uint32(indexData[len(indexData)-4:])
		recordEndPosition = targetSegment.LogFileSize - 1
		// Wait for consumer if no records are available
		time.Sleep(300 * time.Millisecond) // TODO: Make this configurable
	} else {
		indexEntryIndex := getClosestIndexEntryIndex(uint32(offset-targetSegment.StartOffset), indexData)
		log.Debug("GetRecord offset: %v | topic: %v | Closest indexEntryIndex: %v \n\n", offset, topic, indexEntryIndex)

		recordStartPosition = serde.Encoding.Uint32(indexData[indexEntryIndex*8+4:])

		if indexEntryIndex+1 < len(indexData)/8 {
			recordEndPosition = serde.Encoding.Uint32(indexData[(indexEntryIndex+1)*8+4:]) - 1
		} else {
			recordEndPosition = targetSegment.LogFileSize - 1
		}
	}
	log.Debug("GetRecord offset: %v | topic: %v | recordStartPosition: %v recordEndPosition: %v \n\n", offset, topic, recordStartPosition, recordEndPosition)

	_, err = targetSegment.LogFile.Seek(int64(recordStartPosition), io.SeekStart)
	if err != nil {
		return nil, err
	}
	nbRecordByte := recordEndPosition - recordStartPosition + 1
	recordBytes := make([]byte, nbRecordByte)
	n, err := targetSegment.LogFile.Read(recordBytes)
	if n != int(nbRecordByte) || err != nil {
		return nil, fmt.Errorf("error while reading record offset %v, expected %v bytes and read %v bytes. %v", offset, nbRecordByte, n, err)
	}

	log.Debug("ReadRecordBatch %+v", ReadRecords(ReadRecordBatch(recordBytes)))
	return recordBytes, nil
}

// CreateTopic creates a new topic with the given number of partitions and initializes them.
func CreateTopic(name string, numPartitions uint32) error {
	if numPartitions <= 0 {
		return fmt.Errorf("invalid number of partitions")
	}

	for i := 0; i < int(numPartitions); i++ {
		err := os.MkdirAll(GetPartitionDir(name, uint32(i)), 0750)
		if err != nil {
			log.Error("Error creating topic directory: %v", err)
		}
		// Update state
		if i == 0 {
			state.TopicStateInstance[types.TopicName(name)] = make(map[types.PartitionIndex]*types.Partition)
		}
		partition := &types.Partition{
			TopicName: name,
			Index:     uint32(i),
		}
		segment, err := NewSegment(partition)
		if err != nil {
			log.Error("Error creating segment: %v", err)
			return err
		}
		partition.Segments = append(partition.Segments, segment)
		state.TopicStateInstance[types.TopicName(name)][types.PartitionIndex(i)] = partition
	}
	log.Info("Topic %v created with %v partitions", name, numPartitions)
	return nil
}

// EnsurePartition creates a partition if it doesn't exist
func EnsurePartition(topicName string, partitionIndex uint32) error {
	partitionDir := GetPartitionDir(topicName, partitionIndex)
	_, err := os.Stat(partitionDir)
	if err == nil {
		return nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		err := os.MkdirAll(partitionDir, 0750)
		if err != nil {
			log.Error("error creating partition directory: %v", err)
			return err
		}
		if !state.TopicExists(topicName) {
			state.TopicStateInstance[types.TopicName(topicName)] = make(map[types.PartitionIndex]*types.Partition)
		}
		partition := &types.Partition{
			TopicName: topicName,
			Index:     partitionIndex,
		}
		segment, err := NewSegment(partition)
		if err != nil {
			log.Error("Error creating segment: %v", err)
			return err
		}
		partition.Segments = append(partition.Segments, segment)
		state.TopicStateInstance[types.TopicName(topicName)][types.PartitionIndex(partitionIndex)] = partition
		return nil
	}
	return err
}

// GetPartitionDir returns the directory path for the log of a specific partition.
func GetPartitionDir(topic string, partition uint32) string {
	return filepath.Join(state.Config.LogDir, topic+"-"+strconv.Itoa(int(partition)))
}

// SyncPartition syncs the log and index files of the active segment for the given partition.
func SyncPartition(partition *types.Partition) error {
	err := partition.ActiveSegment().LogFile.Sync()
	if err != nil {
		log.Error("Error syncing SegmentFile for %v: %v", partition, err)
		return err
	}
	err = partition.ActiveSegment().IndexFile.Sync()
	if err != nil {
		log.Error("Error syncing IndexFile for %v: %v", partition, err)
		return err
	}
	return nil
}

// FlushDataToDisk synchronously flushes all partition data to disk by syncing their log and index files.
func FlushDataToDisk() {
	for topicName, partitionMap := range state.TopicStateInstance {
		for i, partition := range partitionMap {
			err := SyncPartition(partition)
			if err != nil {
				log.Error("error while flushing partition %v-%v to disk\n", topicName, i)
			}
		}
	}
}

// Startup initializes log management system
func Startup(Config *types.Configuration, shutdown chan bool) {
	state.Config = Config
	_, err := LoadTopicsState()
	if err != nil {
		log.Error("Error loading TopicsState: %v\n", err)
		os.Exit(1)
	}
	go func() {
		flushTicker := time.NewTicker(time.Duration(Config.FlushIntervalMs) * time.Millisecond)
		CleanupTicker := time.NewTicker(time.Duration(Config.LogRetentionCheckIntervalMs) * time.Millisecond)

		defer flushTicker.Stop()
		defer CleanupTicker.Stop()
		for {

			select {
			case <-flushTicker.C:
				FlushDataToDisk()
			case <-CleanupTicker.C:
				CleanupSegments()
			case _, open := <-shutdown:
				if !open {
					log.Info("Stopping log management goroutines")
					return
				}

			}
		}
	}()
}

// Shutdown gracefully shuts down the log management system and closes all segment files.
func Shutdown() {
	log.Info("Storage Shutdown...")

	FlushDataToDisk()
	for _, partitionMap := range state.TopicStateInstance {
		for _, partition := range partitionMap {
			err := partition.ActiveSegment().LogFile.Close()
			if err != nil {
				log.Error("Error syncing SegmentFile: %v", err)
			}
			partition.ActiveSegment().IndexFile.Close()
			if err != nil {
				log.Error("Error syncing IndexFile: %v", err)
			}
		}
	}
}

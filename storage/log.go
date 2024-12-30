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
				log.Error("Error partition index to int:", err)
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
	log.Info("loadTopicsState", state.TopicStateInstance)
	return state.TopicStateInstance, err

}

func ParseRecord(b []byte) types.RecordBatch {
	decoder := serde.NewDecoder(b)
	recordBatch := types.RecordBatch{}
	_ = decoder.CompactArrayLen()
	recordBatch.BaseOffset = decoder.UInt64()
	recordBatch.BatchLength = decoder.UInt32()
	recordBatch.PartitionLeaderEpoch = decoder.UInt32()
	decoder.Offset++
	recordBatch.CRC = decoder.UInt32()
	recordBatch.Attributes = decoder.UInt16()
	recordBatch.LastOffsetDelta = decoder.UInt32()
	recordBatch.BaseTimestamp = decoder.UInt64()
	recordBatch.MaxTimestamp = decoder.UInt64()
	recordBatch.ProducerId = decoder.UInt64()
	recordBatch.ProducerEpoch = decoder.UInt16()
	recordBatch.BaseSequence = decoder.UInt32()
	// recordBatch.Records              []byte //[]Record
	return recordBatch
}

func AppendRecord(topic string, partition uint32, recordBytes []byte) error {
	partitionState := state.GetPartition(topic, partition)
	partitionState.Lock()
	defer partitionState.Unlock()
	activeSegment := partitionState.ActiveSegment()

	// we append to the end
	activeSegment.LogFile.Seek(0, io.SeekEnd)
	activeSegment.IndexFile.Seek(0, io.SeekEnd)

	var newOffset = activeSegment.EndOffset
	if activeSegment.EndOffset > activeSegment.StartOffset { // true if segment is non empty
		newOffset = activeSegment.EndOffset + 1
	}

	decoder := serde.NewDecoder(recordBytes)
	// len of bytes
	_, n := decoder.Uvarint()
	// set the RecordBatch's base offset
	serde.Encoding.PutUint64(recordBytes[n:], uint64(newOffset))

	n, err := activeSegment.LogFile.Write(recordBytes)
	if n != len(recordBytes) || err != nil {
		log.Error("Error while appending record to LogFile for topic %v\n", topic)
	}

	indexEntry := make([]byte, 8)
	serde.Encoding.PutUint32(indexEntry, uint32(newOffset-activeSegment.StartOffset)) // relative to start
	serde.Encoding.PutUint32(indexEntry[4:], activeSegment.LogFileSize)
	activeSegment.IndexFile.Write(indexEntry)

	newIndexData := make([]byte, len(activeSegment.IndexData)+8)
	copy(newIndexData, activeSegment.IndexData)
	copy(newIndexData[len(activeSegment.IndexData):], indexEntry)
	activeSegment.IndexData = newIndexData
	recordBatch := ParseRecord(recordBytes)
	activeSegment.EndOffset = newOffset + uint64(recordBatch.LastOffsetDelta) // the new record batch offset + nb of records in batch
	activeSegment.LogFileSize += uint32(len(recordBytes))
	activeSegment.MaxTimestamp = recordBatch.MaxTimestamp
	log.Debug(" newOffset: %v | NextRecordPosition: %v \n ", newOffset, activeSegment.LogFileSize)
	return nil
}

// binary search through the index entries to find closest offset
// greater than or equal to the given offset
// returns the index entry index
func getClosestIndexEntryIndex(offset uint32, indexData []byte) int {

	left, right := 0, len(indexData)/8-1
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
	// exact offset was not found, we return the closest greater one
	return left
}

func getOffsetSegment(offset uint64, partition *types.Partition) (*types.Segment, error) {
	if offset < partition.StartOffset() || offset > partition.EndOffset()+1 {
		return nil, fmt.Errorf("out of range of offset")
	}
	if offset == partition.EndOffset()+1 { // consumer caught up
		if partition.ActiveSegment().LogFileSize > 0 {
			return partition.ActiveSegment(), nil
		} else {
			// last segment is empty
			segmentBeforeActive := partition.Segments[len(partition.Segments)-2]
			return segmentBeforeActive, nil
		}
	}

	for i, segment := range partition.Segments {
		if offset >= segment.StartOffset && offset <= segment.EndOffset {
			log.Debug("offset %v is within the %v segment bounds [ %v, %v ]", offset, i, segment.StartOffset, segment.EndOffset)
			return segment, nil
		}
	}
	log.Error("mismatch between segments and partition offsets")
	os.Exit(1)
	return nil, nil
}

func GetRecord(offset uint64, topic string, partition uint32) ([]byte, error) {
	log.Debug("GetRecord offset: %v | topic: %v \n\n", offset, topic)
	partitionState := state.GetPartition(topic, partition)
	partitionState.RLock()
	defer partitionState.RUnlock()

	if partitionState.IsEmpty() {
		return nil, nil
	}
	targetSegment, err := getOffsetSegment(offset, partitionState) // partitionState.targetSegment()
	if err != nil {
		return nil, fmt.Errorf("error while getting offset segment: %v", err)
	}
	targetSegment.RLock()
	defer targetSegment.RUnlock()

	indexData := targetSegment.IndexData
	var recordStartPosition, recordEndPosition uint32
	// if offset is greater than latest offset, default to latest record
	if len(indexData)/8 > 0 && offset > targetSegment.EndOffset {
		recordStartPosition = serde.Encoding.Uint32(indexData[len(indexData)-4:])
		recordEndPosition = targetSegment.LogFileSize - 1
		// since there are no records we wait a bit to slow down the consumer
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	} else {

		indexEntryIndex := getClosestIndexEntryIndex(uint32(offset-targetSegment.StartOffset), indexData)

		recordStartPosition = serde.Encoding.Uint32(indexData[indexEntryIndex*8+4:])

		if indexEntryIndex+1 < len(indexData)/8 {
			// there is a later record. Its start pos -1 is the end pos
			recordEndPosition = serde.Encoding.Uint32(indexData[(indexEntryIndex+1)*8+4:]) - 1
		} else {
			// this is the latest record. End pos is end of segment file
			recordEndPosition = targetSegment.LogFileSize - 1
		}
	}
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
	return recordBytes, nil
}

func CreateTopic(name string, numPartitions uint32) error {
	if numPartitions <= 0 {
		return fmt.Errorf("invalid number of partitions")
	}

	for i := 0; i < int(numPartitions); i++ {
		err := os.MkdirAll(GetPartitionDir(name, uint32(i)), 0750)
		if err != nil {
			log.Error("Error creating topic directory:", err)
		}
		// update state
		if i == 0 {
			state.TopicStateInstance[types.TopicName(name)] = make(map[types.PartitionIndex]*types.Partition)
		}
		partition := &types.Partition{
			TopicName: name,
			Index:     uint32(i),
		}
		segment, err := NewSegment(partition)
		if err != nil {
			log.Error("Error creating segment:", err)
			return err
		}
		partition.Segments = []*types.Segment{segment}
		state.TopicStateInstance[types.TopicName(name)][types.PartitionIndex(i)] = partition

	}
	log.Info("Created topic within ", name)
	return nil
}

func GetPartitionDir(topic string, partition uint32) string {
	return filepath.Join(state.Config.LogDir, topic+"-"+strconv.Itoa(int(partition)))
}

func SyncPartition(partition *types.Partition) error {
	err := partition.ActiveSegment().LogFile.Sync()
	if err != nil {
		log.Error("Error syncing SegmentFile for %v:", partition, err)
		return err
	}
	err = partition.ActiveSegment().IndexFile.Sync()
	if err != nil {
		log.Error("Error syncing IndexFile for %v:", partition, err)
		return err
	}
	return nil
}
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

func Startup(Config types.Configuration, shutdown chan bool) {
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

func Shutdown() {
	log.Info("Storage Shutdown...")

	FlushDataToDisk()
	for _, partitionMap := range state.TopicStateInstance {
		for _, partition := range partitionMap {
			err := partition.ActiveSegment().LogFile.Close()
			if err != nil {
				log.Error("Error syncing SegmentFile:", err)
			}
			partition.ActiveSegment().IndexFile.Close()
			if err != nil {
				log.Error("Error syncing IndexFile:", err)
			}
		}
	}
}

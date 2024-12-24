package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/state"
	"github.com/CefBoud/monkafka/types"
)

func LoadTopicsState() (types.TopicsState, error) {
	logDir := state.Config.LogDir
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
				log.Println("Error partition index to int:", err)
				return state.TopicStateInstance, err
			}
			if state.TopicStateInstance[types.TopicName(topicName)] == nil {
				state.TopicStateInstance[types.TopicName(topicName)] = make(map[types.PartitionIndex]*types.Partition)
			}

			indexFile, err := os.OpenFile(getIndexFile(topicName, uint32(index)), os.O_RDWR|os.O_CREATE, 0644) // os.Open(getIndexFile(topicName, uint32(index)))
			if err != nil {
				log.Println("Error opening index file:", err)
				return state.TopicStateInstance, err
			}

			indexData, err := io.ReadAll(indexFile)
			if err != nil {
				return nil, err
			}

			segmentFile, err := os.OpenFile(getSegmentFile(topicName, uint32(index)), os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				log.Println("Error opening segment file:", err)
				return state.TopicStateInstance, err
			}

			stat, err := segmentFile.Stat()
			if err != nil {
				log.Println("Error reading segment file info:", err)
				return state.TopicStateInstance, err
			}
			segmentFileSize := uint32(stat.Size())

			state.TopicStateInstance[types.TopicName(topicName)][types.PartitionIndex(index)] = &types.Partition{SegmentFile: segmentFile, IndexFile: indexFile, IndexData: indexData, NextRecordPosition: segmentFileSize}

			offset, err := getLastOffset(topicName, uint32(index))
			if err != nil {
				log.Println("Error while getting last offset:", err)
				return state.TopicStateInstance, err
			}
			state.TopicStateInstance[types.TopicName(topicName)][types.PartitionIndex(index)].LastOffset = offset

		}
	}
	log.Println("loadTopicsState", state.TopicStateInstance)
	return state.TopicStateInstance, err

}

// for now, we only use one segment with a uint32 offset
// returns the partition's last message offset or -1 if it is empty
func getLastOffset(topic string, partition uint32) (uint32, error) {
	partitionState := state.GetPartition(topic, partition)
	partitionState.RLock()
	defer partitionState.RUnlock()
	a := -1
	MINUS_ONE := uint32(a)
	indexSize := len(partitionState.IndexData)
	if indexSize == 0 { // empty partition => -1 (current is -1, next is 0)
		return MINUS_ONE, nil
	}
	// read the last byte which has the last offset
	var lastOffset uint32 = MINUS_ONE
	if indexSize >= 8 {
		lastRecordOffset := serde.Encoding.Uint32(partitionState.IndexData[indexSize-8:])
		recordPosition := serde.Encoding.Uint32(partitionState.IndexData[indexSize-4:])
		// fmt.Printf(" recordPosition %v partitionState%+v ", recordPosition, partitionState)
		recordBatchHeader := make([]byte, 31) // 31 is enough to get lastOffsetDelta
		_, err := partitionState.SegmentFile.ReadAt(recordBatchHeader, int64(recordPosition))
		if err != nil {
			return 0, fmt.Errorf("getLastOffset: error while partitionState.SegmentFile.ReadAt %v ", err)
		}
		lastOffset = lastRecordOffset + lastOffsetDelta(recordBatchHeader)

	}
	return lastOffset, nil
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
func lastOffsetDelta(recordBatch []byte) uint32 {
	decoder := serde.NewDecoder(recordBatch)
	// len of the RecordBatch is a varint
	decoder.Uvarint()
	decoder.Offset += 8 + 4 + 4 + 1 + 4 + 2 // baseOffset + batchLength 4+ partitionLeaderEpoch 4 + magic 1 + crc 4 + attributes 2
	return decoder.UInt32()
}

func AppendRecord(topic string, partition uint32, recordBytes []byte) error {
	partitionState := state.GetPartition(topic, partition)
	partitionState.Lock()
	defer partitionState.Unlock()
	// we append to the end
	partitionState.SegmentFile.Seek(0, io.SeekEnd)
	partitionState.IndexFile.Seek(0, io.SeekEnd)
	newOffset := partitionState.LastOffset + 1

	decoder := serde.NewDecoder(recordBytes)
	// len of bytes
	_, n := decoder.Uvarint()
	// set the RecordBatch's base offset
	serde.Encoding.PutUint64(recordBytes[n:], uint64(newOffset))

	n, err := partitionState.SegmentFile.Write(recordBytes)
	if n != len(recordBytes) || err != nil {
		log.Printf("Error while appending record to segmentFile for topic %v\n", topic)
	}

	indexEntry := make([]byte, 8)
	serde.Encoding.PutUint32(indexEntry, newOffset)
	serde.Encoding.PutUint32(indexEntry[4:], partitionState.NextRecordPosition)
	// log.Println("indexEntry", indexEntry)
	partitionState.IndexFile.Write(indexEntry)

	newIndexData := make([]byte, len(partitionState.IndexData)+8)
	copy(newIndexData, partitionState.IndexData)
	copy(newIndexData[len(partitionState.IndexData):], indexEntry)
	partitionState.IndexData = newIndexData
	partitionState.LastOffset = newOffset + lastOffsetDelta(recordBytes) // the new record batch offset + nb of records in batch
	partitionState.NextRecordPosition += uint32(len(recordBytes))
	log.Printf(" newOffset: %v | NextRecordPosition: %v \n ", newOffset, partitionState.NextRecordPosition)
	// log.Println("partitionState.LastOffset after", partitionState.LastOffset, lastOffset, state.GetPartition(topic, partition).LastOffset)
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
func GetRecord(offset uint32, topic string, partition uint32) ([]byte, error) {
	// log.Printf("GetRecord offset: %v | topic: %v \n\n", offset, topic)
	partitionState := state.GetPartition(topic, partition)
	partitionState.RLock()
	defer partitionState.RUnlock()
	indexData := partitionState.IndexData
	var recordStartPosition, recordEndPosition uint32
	// if offset is greater than latest offset, default to latest record
	if len(indexData)/8 > 0 && offset > partitionState.LastOffset {
		recordStartPosition = serde.Encoding.Uint32(indexData[len(indexData)-4:])
		recordEndPosition = partitionState.NextRecordPosition - 1
		// since there are no records we wait a bit to slow down the consumer
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	} else {

		indexEntryIndex := getClosestIndexEntryIndex(offset, indexData)
		recordStartPosition = serde.Encoding.Uint32(indexData[indexEntryIndex*8+4:])

		if indexEntryIndex+1 < len(indexData)/8 {
			// there is a later record. Its start pos -1 is the end pos
			recordEndPosition = serde.Encoding.Uint32(indexData[(indexEntryIndex+1)*8+4:]) - 1
		} else {
			// this is the latest record. End pos is end of segment file
			recordEndPosition = partitionState.NextRecordPosition - 1
		}
	}
	_, err := partitionState.SegmentFile.Seek(int64(recordStartPosition), io.SeekStart)
	if err != nil {
		return nil, err
	}
	nbRecordByte := recordEndPosition - recordStartPosition + 1
	recordBytes := make([]byte, nbRecordByte)
	n, err := partitionState.SegmentFile.Read(recordBytes)
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
		err := os.MkdirAll(filepath.Dir(getIndexFile(name, uint32(i))), 0750)
		if err != nil {
			log.Println("Error creating topic directory:", err)
		}
		indexFile, err := os.OpenFile(getIndexFile(name, uint32(i)), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Println("Error creating index file:", err)
			return err
		}
		segmentFile, err := os.OpenFile(getSegmentFile(name, uint32(i)), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Println("Error creating segment file:", err)
			return err
		}
		// update state
		if i == 0 {
			state.TopicStateInstance[types.TopicName(name)] = make(map[types.PartitionIndex]*types.Partition)
		}
		MINUS_ONE := -1
		state.TopicStateInstance[types.TopicName(name)][types.PartitionIndex(i)] = &types.Partition{SegmentFile: segmentFile, IndexFile: indexFile, LastOffset: uint32(MINUS_ONE)}
	}
	log.Println("Created topic within ", name)
	return nil
}

func GetPartitionDir(topic string, partition uint32) string {
	return filepath.Join(state.Config.LogDir, topic+"-"+strconv.Itoa(int(partition)))
}

var FIRST_SEGMENT_FILE = "00000000000000000000"

func getSegmentFile(topic string, partition uint32) string {
	return filepath.Join(GetPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".log")
}
func getIndexFile(topic string, partition uint32) string {
	return filepath.Join(GetPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".index")
}

// TODO: fsync this periodically
func SyncPartition(partition *types.Partition) error {
	err := partition.SegmentFile.Sync()
	if err != nil {
		log.Println("Error syncing SegmentFile:", err)
		return err
	}
	err = partition.IndexFile.Sync()
	if err != nil {
		log.Println("Error syncing IndexFile:", err)
		return err
	}
	return nil
}
func FlushDataToDisk() {
	for topicName, partitionMap := range state.TopicStateInstance {
		for i, partition := range partitionMap {
			err := SyncPartition(partition)
			if err != nil {
				log.Printf("error while flushing partition %v-%v to disk\n", topicName, i)
			}
		}
	}
}

func Startup(Config types.Configuration, shutdown chan bool) {
	state.Config = Config
	_, err := LoadTopicsState()
	if err != nil {
		log.Printf("Error loading TopicsState: %v\n", err)
		os.Exit(1)
	}
	go func() {
		ticker := time.NewTicker(time.Duration(Config.FlushIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				FlushDataToDisk()
			case <-shutdown:
				return
			}
		}
	}()
}

func GracefulShutdown() {
	// flush data to disk
	FlushDataToDisk()
	for _, partitionMap := range state.TopicStateInstance {
		for _, partition := range partitionMap {
			err := partition.SegmentFile.Close()
			if err != nil {
				log.Println("Error syncing SegmentFile:", err)
			}
			partition.IndexFile.Close()
			if err != nil {
				log.Println("Error syncing IndexFile:", err)
			}
		}
	}
}

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

func LoadTopicsState(logDir string) (types.TopicsState, error) {
	state.LogDir = logDir
	entries, err := os.ReadDir(state.LogDir)
	if err != nil {
		return state.TopicStateInstance, err
	}
	// log.Println("entries", entries)
	for _, entry := range entries {
		if entry.IsDir() {
			lastIndex := strings.LastIndex(entry.Name(), "-")
			topicName := entry.Name()[:lastIndex]
			index, err := strconv.Atoi(entry.Name()[lastIndex+1:])
			// log.Println("topicname index lastIndex", topicName, index, lastIndex)
			if err != nil {
				log.Println("Error partition index to int:", err)
				return state.TopicStateInstance, err
			}
			offset, err := getLastOffset(topicName, uint32(index))
			if err != nil {
				log.Println("Error while getting last offset:", err)
				return state.TopicStateInstance, err
			}
			if state.TopicStateInstance[types.TopicName(topicName)] == nil {
				state.TopicStateInstance[types.TopicName(topicName)] = make(map[types.PartitionIndex]types.Offset)
			}
			state.TopicStateInstance[types.TopicName(topicName)][types.PartitionIndex(index)] = types.Offset(offset)

		}
	}
	log.Println("loadTopicsState", state.TopicStateInstance)
	return state.TopicStateInstance, err

}

// for now, we only use one segment with a uint32 offset
func getLastOffset(topic string, partition uint32) (uint32, error) {
	indexFile, err := os.OpenFile(getIndexFile(topic, partition), os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer indexFile.Close()
	stat, _ := indexFile.Stat()
	indexFileSize := stat.Size()
	// read the last byte which has the last offset
	MINUS_ONE := -1
	var lastOffset uint32 = uint32(MINUS_ONE)
	if indexFileSize >= 8 {
		buffer := make([]byte, 4)
		indexFile.Seek(-8, io.SeekEnd)
		_, err := indexFile.Read(buffer)
		if err != nil {
			return 0, err
		}
		lastOffset = serde.Encoding.Uint32(buffer)
	}
	return lastOffset, nil
}
func AppendRecord(topic string, partition uint32, recordBytes []byte) error {
	// TODO: Mutex
	segmentFile, err := os.OpenFile(getSegmentFile(topic, partition), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer segmentFile.Close()
	lastOffset, err := getLastOffset(topic, partition)

	if err != nil {
		return err
	}
	newOffset := lastOffset + 1
	log.Println(" newOffset: ", newOffset)
	serde.Encoding.PutUint64(recordBytes[1:], uint64(newOffset))

	indexFile, err := os.OpenFile(getIndexFile(topic, partition), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	stat, _ := segmentFile.Stat()
	newRecordPosition := stat.Size()
	// log.Println("stat.Size()", stat.Size())
	segmentFile.Write(recordBytes)

	indexEntry := make([]byte, 8)
	serde.Encoding.PutUint32(indexEntry, newOffset)
	serde.Encoding.PutUint32(indexEntry[4:], uint32(newRecordPosition))
	// log.Println("indexEntry", indexEntry)
	indexFile.Write(indexEntry)

	state.TopicStateInstance[types.TopicName(topic)][types.PartitionIndex(partition)] = types.Offset(newOffset)
	log.Println("TopicStateInstance", state.TopicStateInstance)
	// err = segmentFile.Sync()
	// if err != nil {
	// 	return err
	// }
	// err = indexFile.Sync()
	// if err != nil {
	// 	return err
	// }
	return nil
}

func GetRecord(offset uint32, topic string, partition uint32) ([]byte, error) {
	indexData, err := os.ReadFile(getIndexFile(topic, partition))
	if err != nil {
		return nil, err
	}
	var recordStartPosition, recordEndPosition uint32

	// if offset is greater than latest offset, default to latest offset
	if len(indexData)/8 > 0 && offset > serde.Encoding.Uint32(indexData[((len(indexData)/8)-1)*8:]) {
		recordStartPosition = serde.Encoding.Uint32(indexData[((len(indexData)/8)-1)*8+4:])
		size, err := getFileSize(getSegmentFile(topic, partition))
		if err != nil {
			return nil, err
		}
		recordEndPosition = uint32(size) - 1
		// since there are no records we wait a bit to slow down the consumer
		time.Sleep(300 * time.Millisecond) // TODO get this from consumer settings
	} else {
		var searchOffset uint32
		// TODO use binary search
		for i := 0; i < len(indexData)/8; i++ {
			searchOffset = serde.Encoding.Uint32(indexData[i*8:])
			if searchOffset == offset {
				recordStartPosition = serde.Encoding.Uint32(indexData[i*8+4:])

				if i+1 < len(indexData)/8 {
					recordEndPosition = serde.Encoding.Uint32(indexData[(i+1)*8+4:]) - 1
				} else {
					size, err := getFileSize(getSegmentFile(topic, partition))
					if err != nil {
						return nil, err
					}
					recordEndPosition = uint32(size) - 1
				}
				break
			}
		}
	}

	log.Printf("offset %v recordStartPosition  %v recordEndPosition %v\n", offset, recordStartPosition, recordEndPosition)
	segmentFile, err := os.OpenFile(getSegmentFile(topic, partition), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer segmentFile.Close()
	_, err = segmentFile.Seek(int64(recordStartPosition), io.SeekStart)
	if err != nil {
		return nil, err
	}
	nbRecordByte := recordEndPosition - recordStartPosition + 1
	recordBytes := make([]byte, nbRecordByte)
	n, err := segmentFile.Read(recordBytes)
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
		// update state
		if i == 0 {
			state.TopicStateInstance[types.TopicName(name)] = make(map[types.PartitionIndex]types.Offset)
		}
		state.TopicStateInstance[types.TopicName(name)][types.PartitionIndex(i)] = types.Offset(0)
	}
	log.Println("Created topic within ", name)
	return nil
}

func GetPartitionDir(topic string, partition uint32) string {
	return filepath.Join(state.LogDir, topic+"-"+strconv.Itoa(int(partition)))
}

var FIRST_SEGMENT_FILE = "00000000000000000000"

func getSegmentFile(topic string, partition uint32) string {
	return filepath.Join(GetPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".log")
}
func getIndexFile(topic string, partition uint32) string {
	return filepath.Join(GetPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".index")
}

func getFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

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
	var lastOffset uint32 = uint32(MINUS_ONE)
	if indexFileSize >= 8 {
		buffer := make([]byte, 4)
		indexFile.Seek(-8, io.SeekEnd)
		_, err := indexFile.Read(buffer)
		if err != nil {
			return 0, err
		}
		lastOffset = Encoding.Uint32(buffer)
	}
	return lastOffset, nil
}
func AppendRecord(topic string, partition uint32, recordBytes []byte) error {
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
	fmt.Println(" newOffset", newOffset)
	Encoding.PutUint64(recordBytes[1:], uint64(newOffset))

	indexFile, err := os.OpenFile(getIndexFile(topic, partition), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	stat, _ := segmentFile.Stat()
	newRecordPosition := stat.Size()
	fmt.Println("stat.Size()", stat.Size())
	segmentFile.Write(recordBytes)

	indexEntry := make([]byte, 8)
	Encoding.PutUint32(indexEntry, newOffset)
	Encoding.PutUint32(indexEntry[4:], uint32(newRecordPosition))
	fmt.Println("indexEntry", indexEntry)
	indexFile.Write(indexEntry)

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
	if len(indexData)/8 > 0 && offset > Encoding.Uint32(indexData[((len(indexData)/8)-1)*8:]) {
		recordStartPosition = Encoding.Uint32(indexData[((len(indexData)/8)-1)*8+4:])
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
			searchOffset = Encoding.Uint32(indexData[i*8:])
			if searchOffset == offset {
				recordStartPosition = Encoding.Uint32(indexData[i*8+4:])

				if i+1 < len(indexData)/8 {
					recordEndPosition = Encoding.Uint32(indexData[(i+1)*8+4:]) - 1
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

	fmt.Printf("offset %v recordStartPosition  %v recordEndPosition %v\n", offset, recordStartPosition, recordEndPosition)
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

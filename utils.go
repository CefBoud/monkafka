package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
)

func NowAsUnixMilli() uint64 {

	return uint64(MINUS_ONE) //uint64(time.Now().UnixNano() / 1e6)
}

func getPartitionDir(topic string, partition uint32) string {
	return filepath.Join(logDir, topic+"-"+strconv.Itoa(int(partition)))
}

var FIRST_SEGMENT_FILE = "00000000000000000000"

func getSegmentFile(topic string, partition uint32) string {
	return filepath.Join(getPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".log")
}
func getIndexFile(topic string, partition uint32) string {
	return filepath.Join(getPartitionDir(topic, partition), FIRST_SEGMENT_FILE+".index")
}

func getLastLine(filename string) (string, error) {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lastLine string
	// Read the file line by line
	for scanner.Scan() {
		lastLine = scanner.Text() // Update the lastLine with the current line
	}
	// Check if there was any error while reading
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return lastLine, nil
}

func ReadLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func getFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

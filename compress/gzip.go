package compress

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	log "github.com/CefBoud/monkafka/logging"
)

var (
	gzipWriterPool = sync.Pool{

		New: func() any {
			return gzip.NewWriter(nil)
		},
	}
	// we don't rely on New because gzip.NewReader
	// can return an error
	gzipReaderPool sync.Pool
)

// GzipCompressor implements Compressor interface
type GzipCompressor struct{} // TODO: handle the various GZIP levels (only default now)

// Compress takes in data and applies gzip to it
func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {

	var compressedData bytes.Buffer
	gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(gzipWriter)
	gzipWriter.Reset(&compressedData)
	// gzipWriter := gzip.NewWriter(&compressedData)

	_, err := gzipWriter.Write(data)
	if err != nil {
		log.Error("Failed to compress data: %v", err)
		return nil, err

	}

	err = gzipWriter.Close()
	if err != nil {
		log.Error("Failed to close GZIP writer: %v", err)
		return nil, err
	}

	return compressedData.Bytes(), nil
}

// Decompress decompresses gzip-compressed data
func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	var err error
	gzipReader, found := gzipReaderPool.Get().(*gzip.Reader)
	bytesReader := bytes.NewReader(data)
	if found {
		err = gzipReader.Reset(bytesReader)
	} else {
		// pool is empty, we init a new reader
		gzipReader, err = gzip.NewReader(bytesReader)
	}
	if err != nil {
		return nil, err
	}

	defer gzipReaderPool.Put(gzipReader)

	decompressedData, err := io.ReadAll(gzipReader)
	if err != nil {
		log.Error("Failed to decompress data: %v", err)
		return nil, err
	}
	err = gzipReader.Close()
	if err != nil {
		log.Error("Failed to close GZIP reader: %v", err)
		return nil, err
	}

	return decompressedData, nil
}

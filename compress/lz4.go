package compress

import (
	"bytes"
	"sync"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/pierrec/lz4/v4"
)

// LZ4Compressor implements Compressor interface
type LZ4Compressor struct{}

// Multiple instances of writers and readers can put pressure on the memory,
// we use a sync pool to reuse instance instead on letting the GC deal with them
var (
	LZ4WriterPool = sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	LZ4ReaderPool = sync.Pool{
		New: func() any {
			return lz4.NewReader(nil)
		},
	}
)

// Compress takes in data and applies LZ4 to it
func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := LZ4WriterPool.Get().(*lz4.Writer)
	writer.Reset(&buf)
	defer LZ4WriterPool.Put(writer)
	_, err := writer.Write(data)
	if err != nil {
		log.Error("Failed to compress data: %v", err)
	}
	err = writer.Close()
	if err != nil {
		log.Error("Failed to close LZ4 writer: %v", err)
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress decompresses LZ4-compressed data
func (c *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	reader := LZ4ReaderPool.Get().(*lz4.Reader)
	reader.Reset(bytes.NewReader(data))
	defer LZ4ReaderPool.Put(reader)

	var decompressedData bytes.Buffer
	_, err := decompressedData.ReadFrom(reader)
	if err != nil {
		log.Error("Failed to decompress data: %v", err)
		return nil, err
	}

	return decompressedData.Bytes(), nil
}

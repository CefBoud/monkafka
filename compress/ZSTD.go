package compress

import (
	"sync"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/klauspost/compress/zstd"
)

// ZSTDCompressor implements Compressor interface
type ZSTDCompressor struct{} // TODO: add compression levels

// Multiple instances of writers and readers can put pressure on the memory,
// we use a sync pool to reuse instance instead on letting the GC deal with them
var (
	zstdWriterPool, zstdReaderPool sync.Pool
)

// Compress takes in data and applies ZSTD to it
func (c *ZSTDCompressor) Compress(data []byte) ([]byte, error) {
	var res []byte
	var err error
	encoder, found := zstdWriterPool.Get().(*zstd.Encoder)
	if !found {
		// WithZeroFrames will encode 0 length input as full frames. This is needed for compatibility with zstandard usage
		encoder, err = zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	}
	defer zstdWriterPool.Put(encoder)

	res = encoder.EncodeAll(data, nil)
	log.Debug("encoder.EncodeAll \n%v \n%v ", data, res)
	if err != nil {
		log.Error("Failed to compress data: %v", err)
		return nil, err
	}
	err = encoder.Close()
	if err != nil {
		log.Error("Failed to close ZSTD encoder: %v", err)
		return nil, err
	}

	return res, nil
}

// Decompress decompresses ZSTD-compressed data
func (c *ZSTDCompressor) Decompress(data []byte) ([]byte, error) {
	var err error
	decoder, found := zstdReaderPool.Get().(*zstd.Decoder)
	if !found {
		// pool is empty, we init a new reader
		decoder, err = zstd.NewReader(nil)
	}
	if err != nil {
		return nil, err
	}
	defer zstdReaderPool.Put(decoder)

	decompressedData, err := decoder.DecodeAll(data, nil)
	if err != nil {
		log.Error("Failed to decompress data: %v", err)
		return nil, err
	}
	return decompressedData, nil
}

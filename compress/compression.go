package compress

// CompressionType represents one of the supported compression types
type CompressionType uint8

// Kafka compression types
const (
	NONE   CompressionType = 0
	GZIP   CompressionType = 1
	SNAPPY CompressionType = 2
	LZ4    CompressionType = 3
	ZSTD   CompressionType = 4
)

var compressors = map[CompressionType]Compressor{
	NONE:   nil,
	GZIP:   &GzipCompressor{},
	SNAPPY: &SnappyCompressor{},
	LZ4:    &LZ4Compressor{},
	ZSTD:   &ZSTDCompressor{},
}

// GetCompressor returns the Compressor based on the RecordBatch attributes
func GetCompressor(attribute uint16) Compressor {
	compressionType := CompressionType(attribute & 0x07) // first 3 bits: 0~2
	return compressors[compressionType]
}

// Compressor represents one of the supported compressors
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

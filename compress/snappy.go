package compress

// xerial snappy is the Java implementation of Google's snappy algo
// It is used by Kafka. This java implem adds a special framing format
// not supported by github.com/golang/snappy which go-xerial-snappy wraps and handles
import snappy "github.com/eapache/go-xerial-snappy"

// SnappyCompressor implements Compressor interface
type SnappyCompressor struct{}

// Compress takes in data and applies snappy to it
func (c *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(data), nil
}

// Decompress decompresses snappy-compressed data
func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(data)
}

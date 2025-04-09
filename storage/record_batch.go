package storage

import (
	"hash/crc32"

	"github.com/CefBoud/monkafka/compress"
	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

// const LogOverhead = 12 // offset field 8 + length field 4

// ReadRecordBatch turns bytes into a RecordBatch struct
func ReadRecordBatch(b []byte) types.RecordBatch {
	decoder := serde.NewDecoder(b)
	recordBatch := types.RecordBatch{}
	recordBatch.BaseOffset = decoder.UInt64()
	recordBatch.BatchLength = decoder.UInt32()
	recordBatch.PartitionLeaderEpoch = decoder.UInt32()
	recordBatch.Magic = decoder.UInt8()
	if recordBatch.Magic != 2 {
		log.Error(`recordBatch Magic is %v. MonKafka only supports magic=2 i.e. record batches for now.
		v0 and v1 are message sets. Refer to https://kafka.apache.org/documentation/#messageset`,
			recordBatch.Magic)
		return recordBatch
	}
	recordBatch.CRC = decoder.UInt32()
	recordBatch.Attributes = decoder.UInt16()
	recordBatch.LastOffsetDelta = decoder.UInt32()
	recordBatch.BaseTimestamp = decoder.UInt64()
	recordBatch.MaxTimestamp = decoder.UInt64()
	recordBatch.ProducerID = decoder.UInt64()
	recordBatch.ProducerEpoch = decoder.UInt16()
	recordBatch.BaseSequence = decoder.UInt32()
	recordBatch.NumRecord = decoder.UInt32()

	recordBatch.Records = decoder.GetRemainingBytes()
	log.Trace("ReadRecordBatch recordBatch %+v", recordBatch)
	return recordBatch
}

// ReadRecords transforms RecordBatch Record bytes into a slice of Record struct
func ReadRecords(rb types.RecordBatch) []types.Record {
	var records []types.Record
	recordBytes := rb.Records

	if compressor := compress.GetCompressor(rb.Attributes); compressor != nil {
		decompressed, err := compressor.Decompress(recordBytes)
		if err != nil {
			log.Error("error while decompressing RecordBatch %v. Error: %v", rb, err)
		} else {
			// TODO: if decompression fails, we still return the corrupt bytes. Better fail?
			recordBytes = decompressed
		}
	}

	decoder := serde.NewDecoder(recordBytes)
	for i := 0; i < int(rb.NumRecord); i++ {
		_, _ = decoder.Varint() // length
		record := types.Record{Attributes: int8(decoder.UInt8())}
		record.TimestampDelta, _ = decoder.Varint()
		record.OffsetDelta, _ = decoder.Varint()
		numBytes, _ := decoder.Varint() // VARINT NOT *U*VARINT
		record.Key = decoder.GetNBytes(int(numBytes))
		numBytes, _ = decoder.Varint() // VARINT NOT *U*VARINT
		record.Value = decoder.GetNBytes(int(numBytes))
		numBytes, _ = decoder.Varint()   // headers length
		decoder.GetNBytes(int(numBytes)) // TODO: parse headers
		records = append(records, record)
	}
	return records
}

// NewRecordBatch creates a RecordBatch given the key and value bytes
// TODO: handle multi records batches and compression
func NewRecordBatch(recordKey []byte, recordValue []byte, attributes uint16) types.RecordBatch {
	MinusOne := -1
	currentTimestamp := utils.NowAsUnixMilli()
	rb := types.RecordBatch{
		Magic: 2,
		// no compression / (timestampType == CREATE_TIME)
		Attributes: attributes, //3 first bit determine compression: 4 == ZSTD , // 1 == GZIP //
		// defaults
		ProducerID:           uint64(MinusOne),
		ProducerEpoch:        uint16(MinusOne),
		BaseSequence:         uint32(MinusOne),
		PartitionLeaderEpoch: uint32(MinusOne),
		BaseTimestamp:        currentTimestamp,
		MaxTimestamp:         currentTimestamp,
	}
	// records
	encoder := serde.NewEncoder()
	encoder.PutInt8(0)   // attributes (unused)
	encoder.PutVarint(0) // timestampDelta
	encoder.PutVarint(0) // offsetDelta
	log.Debug("len(recordKey) %v recordKey %v, len(recordValue) %v recordValue %v", len(recordKey), recordKey, len(recordValue), recordValue)
	encoder.PutVarint(len(recordKey))
	encoder.PutBytes(recordKey)
	encoder.PutVarint(len(recordValue))
	encoder.PutBytes(recordValue)
	encoder.PutVarint(0) // no headers
	encoder.PutVarIntLen()

	rb.Records = encoder.Bytes()

	if compressor := compress.GetCompressor(rb.Attributes); compressor != nil { // 1 == gzip
		log.Info("NewRecordBatch recordByte length s %v compressor %T", len(rb.Records), compressor)
		compressed, err := compressor.Compress(rb.Records)
		if err != nil {
			log.Error("error while compressing RecordBatch %v. Error: %v", rb, err)
		}
		log.Debug("NewRecordBatch After compression recordBytes length %v", len(compressed))
		rb.Records = compressed
	}

	return rb
}

// WriteRecordBatch encodes a record batch into bytes
func WriteRecordBatch(rb types.RecordBatch) []byte {
	// attributes after CRC that will be check summed
	encoder := serde.NewEncoder()
	encoder.PutInt16(rb.Attributes)
	encoder.PutInt32(rb.LastOffsetDelta)
	encoder.PutInt64(rb.BaseTimestamp)
	encoder.PutInt64(rb.MaxTimestamp)
	encoder.PutInt64(rb.ProducerID)
	encoder.PutInt16(rb.ProducerEpoch)
	encoder.PutInt32(rb.BaseSequence)
	encoder.PutInt32(1) // nb records
	encoder.PutBytes(rb.Records)

	checkSummedBytes := encoder.Bytes()
	// Calculate checksum
	CRC := crc32.Checksum(checkSummedBytes, crc32.MakeTable(crc32.Castagnoli))

	length := uint32(len(checkSummedBytes) + 9) // Length is all check summed bytes + partitionLeaderEpoch 4 + magic 1  + crc 4
	// encode final bytes now that we have the CRC
	encoder = serde.NewEncoder()
	encoder.PutInt64(rb.BaseOffset) // will be updated on actual append
	encoder.PutInt32(length)        // or expressed differently: whole size - LogOverhead  (offset field 8 + length field 4)
	encoder.PutInt32(rb.PartitionLeaderEpoch)
	encoder.PutInt8(rb.Magic) // magic byte
	encoder.PutInt32(CRC)
	encoder.PutBytes(checkSummedBytes)
	return encoder.Bytes()
}

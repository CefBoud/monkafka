package storage

import (
	"hash/crc32"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/types"
	"github.com/CefBoud/monkafka/utils"
)

const LOG_OVERHEAD = 12 // offset field 8 + length field 4

func ReadRecordBatch(b []byte) types.RecordBatch {
	decoder := serde.NewDecoder(b)
	recordBatch := types.RecordBatch{}
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
	recordBatch.NumRecord = decoder.UInt32()
	numBytes, _ := decoder.Varint() // VARINT NOT *U*VARINT
	recordBatch.Records = decoder.GetNBytes(int(numBytes))
	// log.Debug("ReadRecordBatch recordBatch %+v", recordBatch)
	return recordBatch
}

func ReadRecord(b []byte) types.Record {
	decoder := serde.NewDecoder(b)
	record := types.Record{Attributes: int8(decoder.UInt8())}

	record.TimestampDelta, _ = decoder.Varint()
	record.OffsetDelta, _ = decoder.Varint()
	numBytes, _ := decoder.Varint() // VARINT NOT *U*VARINT
	record.Key = decoder.GetNBytes(int(numBytes))
	numBytes, _ = decoder.Varint() // VARINT NOT *U*VARINT
	record.Value = decoder.GetNBytes(int(numBytes))
	// TODO: handle headers
	// log.Debug("ReadRecord: record: %+v", record)
	return record
}

// record batch of just 1 record (for now)
// TODO: handle multi records batches and compression
func NewRecordBatch(recordKey []byte, recordValue []byte) types.RecordBatch {
	MINUS_ONE := -1
	currentTimestamp := utils.NowAsUnixMilli()
	rb := types.RecordBatch{
		Magic: 2,
		// no compression / (timestampType == CREATE_TIME)
		Attributes: 0, // For now, with compression and transactions, this needs to change
		// defaults
		ProducerId:           uint64(MINUS_ONE),
		ProducerEpoch:        uint16(MINUS_ONE),
		BaseSequence:         uint32(MINUS_ONE),
		PartitionLeaderEpoch: uint32(MINUS_ONE),
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

	return rb
}
func WriteRecordBatch(rb types.RecordBatch) []byte {
	// attributes after CRC that will be check summed
	encoder := serde.NewEncoder()
	encoder.PutInt16(rb.Attributes)
	encoder.PutInt32(rb.LastOffsetDelta)
	encoder.PutInt64(rb.BaseTimestamp)
	encoder.PutInt64(rb.MaxTimestamp)
	encoder.PutInt64(rb.ProducerId)
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
	encoder.PutInt32(length)        // or expressed differently: whole size - LOG_OVERHEAD  (offset field 8 + length field 4)
	encoder.PutInt32(rb.PartitionLeaderEpoch)
	encoder.PutInt8(rb.Magic) // magic byte
	encoder.PutInt32(CRC)
	encoder.PutBytes(checkSummedBytes)
	return encoder.Bytes()
}

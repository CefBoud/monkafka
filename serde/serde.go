package serde

import (
	"encoding/binary"
	"slices"

	"github.com/CefBoud/monkafka/types"
)

var Encoding = binary.BigEndian

type Encoder struct {
	b      []byte
	offset int
}

// 64 KiB
const BUFFER_INCREMENT = 16384 * 4

func NewEncoder() Encoder {
	return Encoder{b: make([]byte, BUFFER_INCREMENT)}
}

func (e *Encoder) ensureBufferSpace(off int) {
	if off+e.offset > len(e.b) {
		newBuffer := make([]byte, len(e.b)+BUFFER_INCREMENT)
		copy(newBuffer, e.b)
		e.b = newBuffer
	}
}
func (e *Encoder) PutInt32(i uint32) {
	e.ensureBufferSpace(4)
	Encoding.PutUint32(e.b[e.offset:], i)
	e.offset += 4
}
func (e *Encoder) PutInt64(i uint64) {
	e.ensureBufferSpace(8)
	Encoding.PutUint64(e.b[e.offset:], i)
	e.offset += 8
}
func (e *Encoder) PutInt16(i uint16) {
	e.ensureBufferSpace(2)
	Encoding.PutUint16(e.b[e.offset:], i)
	e.offset += 2
}
func (e *Encoder) PutInt8(i uint8) {
	e.ensureBufferSpace(1)
	e.b[e.offset] = byte(i)
	e.offset++
}

func (e *Encoder) PutBool(b bool) {
	e.ensureBufferSpace(1)
	e.b[e.offset] = byte(0)
	if b {
		e.b[e.offset] = byte(1)
	}
	e.offset++
}

func (e *Encoder) PutUvarint(l int) {
	e.ensureBufferSpace(4 + l)
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l))
}
func (e *Encoder) PutVarint(l int) {
	e.ensureBufferSpace(4 + l)
	e.offset += binary.PutVarint(e.b[e.offset:], int64(l))
}

func (e *Encoder) PutString(s string) {
	e.ensureBufferSpace(2 + len(s))
	e.PutInt16(uint16(len(s)))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}

func (e *Encoder) PutCompactString(s string) {
	e.ensureBufferSpace(4 + len(s)) // meh?
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(s)+1))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}
func (e *Encoder) PutBytes(b []byte) {
	e.ensureBufferSpace(len(b))
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

func (e *Encoder) PutCompactBytes(b []byte) {
	e.ensureBufferSpace(4 + len(b))
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(b)+1))
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

func (e *Encoder) PutCompactArrayLen(l int) {
	e.ensureBufferSpace(4 + l) //  meh as well?
	// nil arrays should give -1
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l+1))
}
func (e *Encoder) PutLen() {
	lengthBytes := Encoding.AppendUint32([]byte{}, uint32(e.offset))
	e.b = slices.Insert(e.b, 0, lengthBytes...)
	e.offset += len(lengthBytes)
}

// put len as a varint at the start of the buffer
func (e *Encoder) PutVarIntLen() {
	lengthBytes := binary.AppendVarint([]byte{}, int64(e.offset))
	e.b = slices.Insert(e.b, 0, lengthBytes...)
	e.offset += len(lengthBytes)
}

func (e *Encoder) EndStruct() {
	e.ensureBufferSpace(1)
	// add 0 to indicate to end of tagged fields KIP-482
	e.b[e.offset] = 0
	e.offset++
}
func (e *Encoder) Bytes() []byte {
	return e.b[:e.offset]
}

func (e *Encoder) FinishAndReturn() []byte {
	e.EndStruct() // close struct
	e.PutLen()    // put the final length
	return e.Bytes()
}

func ParseHeader(buffer []byte, connAddr string) types.Request {
	clientIdLen := Encoding.Uint16(buffer[12:])

	return types.Request{
		Length:            Encoding.Uint32(buffer),
		RequestApiKey:     Encoding.Uint16(buffer[4:]),
		RequestApiVersion: Encoding.Uint16(buffer[6:]),
		CorrelationID:     Encoding.Uint32(buffer[8:]),
		ClientId:          string(buffer[14 : 14+clientIdLen]),
		ConnectionAddress: connAddr,
		Body:              buffer[14+clientIdLen+1:], // + 1 to for empty _tagged_fields
	}
}

type Decoder struct {
	b      []byte
	Offset int
}

func NewDecoder(b []byte) Decoder {
	return Decoder{b: b}
}

func (d *Decoder) UInt32() uint32 {
	res := Encoding.Uint32(d.b[d.Offset:])
	d.Offset += 4
	return res
}
func (d *Decoder) UInt64() uint64 {
	res := Encoding.Uint64(d.b[d.Offset:])
	d.Offset += 8
	return res
}

func (d *Decoder) UInt16() uint16 {
	res := Encoding.Uint16(d.b[d.Offset:])
	d.Offset += 2
	return res
}

func (d *Decoder) UInt8() uint8 {
	res := uint8(d.b[d.Offset])
	d.Offset++
	return res
}

func (d *Decoder) Bool() bool {
	res := false
	if d.b[d.Offset] > 0 {
		res = true
	}
	d.Offset++
	return res
}
func (d *Decoder) UUID() [16]byte {
	uuid := d.b[d.Offset : d.Offset+16]
	d.Offset += 16
	return [16]byte(uuid)
}

func (d *Decoder) String() string {
	stringLen := d.UInt16()
	if stringLen == 0 { // nullable string
		return ""
	}
	res := string(d.b[d.Offset : d.Offset+int(stringLen)])
	d.Offset += int(stringLen)
	return res
}

func (d *Decoder) CompactString() string {
	stringLen, n := binary.Uvarint(d.b[d.Offset:])
	d.Offset += n
	if stringLen == 0 { // nullable string
		return ""
	}
	stringLen--

	res := string(d.b[d.Offset : d.Offset+int(stringLen)])
	d.Offset += int(stringLen)
	return res
}

func (d *Decoder) Bytes() []byte {
	bytesLen := int(d.CompactArrayLen())
	res := d.b[d.Offset : d.Offset+bytesLen]
	d.Offset += bytesLen
	return res
}
func (d *Decoder) CompactBytes() []byte {
	bytesLen, n := binary.Uvarint(d.b[d.Offset:])
	bytesLen--
	d.Offset += n
	if bytesLen < 1 {
		return []byte{}
	}
	res := d.b[d.Offset : d.Offset+int(bytesLen)]
	d.Offset += int(bytesLen)
	return res
}

func (d *Decoder) GetNBytes(n int) []byte {
	res := d.b[d.Offset : d.Offset+int(n)]
	d.Offset += int(n)
	return res
}

func (d *Decoder) CompactArrayLen() uint64 {
	arrayLen, n := binary.Uvarint(d.b[d.Offset:])
	arrayLen--
	d.Offset += n
	return arrayLen
}

func (d *Decoder) Uvarint() (uint64, int) {
	varint, n := binary.Uvarint(d.b[d.Offset:])
	d.Offset += n
	return varint, n
}

func (d *Decoder) Varint() (int64, int) {
	varint, n := binary.Varint(d.b[d.Offset:])
	d.Offset += n
	return varint, n
}

func (d *Decoder) EndStruct() {
	//  end of empty tagged fields KIP-482
	d.Offset++
}

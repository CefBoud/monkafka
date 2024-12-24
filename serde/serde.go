package serde

import (
	"encoding/binary"

	"github.com/CefBoud/monkafka/types"
)

var Encoding = binary.BigEndian

type Encoder struct {
	b      []byte
	offset int
}

func NewEncoder() Encoder {
	// TODO handle byte slice limit
	return Encoder{b: make([]byte, 8192), offset: 4} // start at 4 to leave space for length uint32
}
func (e *Encoder) PutInt32(i uint32) {
	Encoding.PutUint32(e.b[e.offset:], i)
	e.offset += 4
}
func (e *Encoder) PutInt64(i uint64) {
	Encoding.PutUint64(e.b[e.offset:], i)
	e.offset += 8
}
func (e *Encoder) PutInt16(i uint16) {
	Encoding.PutUint16(e.b[e.offset:], i)
	e.offset += 2
}
func (e *Encoder) PutBool(b bool) {
	e.b[e.offset] = byte(0)
	if b {
		e.b[e.offset] = byte(1)
	}
	e.offset++
}
func (e *Encoder) PutString(s string) {
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(s)+1))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}
func (e *Encoder) PutBytes(b []byte) {
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

func (e *Encoder) PutCompactArrayLen(l int) {
	// nil arrays should give -1
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l+1))
}
func (e *Encoder) PutLen() {
	Encoding.PutUint32(e.b, uint32(e.offset-4))
}
func (e *Encoder) EndStruct() {
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
	req := types.Request{}
	req.Length = Encoding.Uint32(buffer)
	req.RequestApiKey = Encoding.Uint16(buffer[4:])
	req.RequestApiVersion = Encoding.Uint16(buffer[6:])
	req.CorrelationID = Encoding.Uint32(buffer[8:])
	clientIdLen := Encoding.Uint16(buffer[12:])
	req.ClientId = string(buffer[14 : 14+clientIdLen])
	req.ConnectionAddress = connAddr
	req.Buffer = buffer
	return req
}

type Decoder struct {
	b      []byte
	Offset int
}

func NewDecoder(b []byte) Decoder {
	return Decoder{b: b}
}

func (d *Decoder) SkipHeader() {
	// msg len 4 + api key 2 + api version 2 + correlation id 4
	// + len(client_id) 2 + client_id X +  no tags 1
	headerLength := 4 + 2 + 2 + 4 + 2 + Encoding.Uint16(d.b[12:]) + 1
	d.Offset += int(headerLength)
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
func (d *Decoder) BytesWithLen() []byte {
	bytesLen, n := binary.Uvarint(d.b[d.Offset:])
	bytesLen--
	res := d.b[d.Offset : d.Offset+int(bytesLen)+n]
	d.Offset += int(bytesLen) + n
	return res
}

func (d *Decoder) CompactArrayLen() uint64 {
	arrayLen, n := binary.Uvarint(d.b[d.Offset:])
	arrayLen--
	d.Offset += n
	return arrayLen
}

func (d *Decoder) EndStruct() {
	//  end of empty tagged fields KIP-482
	d.Offset++
}

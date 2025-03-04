package serde

import (
	"encoding/binary"
	"reflect"
	"slices"

	log "github.com/CefBoud/monkafka/logging"
	"github.com/CefBoud/monkafka/types"
)

// Encoding is Big Endian as per the protocol
var Encoding = binary.BigEndian

// Encoder is a byte slice with an offset
type Encoder struct {
	b      []byte // Buffer to hold encoded data
	offset int    // Current position in the buffer
}

// BufferIncrement is 64 KiB and represents the size of increment when buffer limit is reached
const BufferIncrement = 16384 * 4 // Buffer size increment when resizing

// NewEncoder creates a new Encoder with an initial buffer
func NewEncoder() Encoder {
	return Encoder{b: make([]byte, BufferIncrement)}
}

// ensureBufferSpace ensures the buffer has enough space to accommodate the new data
func (e *Encoder) ensureBufferSpace(off int) {
	if off+e.offset > len(e.b) {
		newBuffer := make([]byte, len(e.b)+BufferIncrement)
		copy(newBuffer, e.b)
		e.b = newBuffer
	}
}

// Encode encodes a struct using reflection
func (e *Encoder) Encode(x any) {
	t := reflect.TypeOf(x)
	v := reflect.ValueOf(x)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)
		if field.Type.Kind() == reflect.Slice {
			e.PutCompactArrayLen(value.Len())
			if field.Type.Elem().Kind() == reflect.Struct { // Slice of structs
				for j := 0; j < value.Len(); j++ {
					e.Encode(value.Index(j).Interface())
				}
			} else { // Slice of basic types
				for j := 0; j < value.Len(); j++ {
					e.Put(value.Index(j).Interface())
				}
			}
		} else if field.Type.Kind() == reflect.Struct {
			e.Encode(value.Interface())
		} else {
			e.Put(value.Interface())
		}
	}
	e.EndStruct()
}

// Put encodes input based on its type
func (e *Encoder) Put(i any, a ...any) {
	switch c := i.(type) {
	case bool:
		e.PutBool(c)
	case uint8:
		e.PutInt8(c)
	case uint16:
		e.PutInt16(c)
	case uint32:
		e.PutInt32(c)
	case uint64:
		e.PutInt64(c)
	case int:
		e.PutVarint(c)
	case uint:
		e.PutUvarint(c)
	case string:
		e.PutCompactString(c)
	case []byte:
		e.PutCompactBytes(c)
	case [16]byte:
		e.PutBytes(c[:])
	default:
		log.Panic("Unknown type %T", c)
	}
}

// PutInt32 encodes a uint32 value into the buffer
func (e *Encoder) PutInt32(i uint32) {
	e.ensureBufferSpace(4)
	Encoding.PutUint32(e.b[e.offset:], i)
	e.offset += 4
}

// PutInt64 encodes a uint64 value into the buffer
func (e *Encoder) PutInt64(i uint64) {
	e.ensureBufferSpace(8)
	Encoding.PutUint64(e.b[e.offset:], i)
	e.offset += 8
}

// PutInt16 encodes a uint16 value into the buffer
func (e *Encoder) PutInt16(i uint16) {
	e.ensureBufferSpace(2)
	Encoding.PutUint16(e.b[e.offset:], i)
	e.offset += 2
}

// PutInt8 encodes a uint8 value into the buffer
func (e *Encoder) PutInt8(i uint8) {
	e.ensureBufferSpace(1)
	e.b[e.offset] = byte(i)
	e.offset++
}

// PutBool encodes a boolean value into the buffer
func (e *Encoder) PutBool(b bool) {
	e.ensureBufferSpace(1)
	e.b[e.offset] = byte(0)
	if b {
		e.b[e.offset] = byte(1)
	}
	e.offset++
}

// PutUvarint encodes a length as an unsigned varint
func (e *Encoder) PutUvarint(l uint) {
	e.ensureBufferSpace(5 + int(l))
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l))
}

// PutVarint encodes a length as a signed varint
func (e *Encoder) PutVarint(l int) {
	e.ensureBufferSpace(5 + l)
	e.offset += binary.PutVarint(e.b[e.offset:], int64(l))
}

// PutString encodes a string (length + content) into the buffer
func (e *Encoder) PutString(s string) {
	e.ensureBufferSpace(2 + len(s))
	e.PutInt16(uint16(len(s)))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}

// PutCompactString encodes a string using a compressed length format
func (e *Encoder) PutCompactString(s string) {
	e.ensureBufferSpace(5 + len(s))
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(s)+1))
	copy(e.b[e.offset:], s)
	e.offset += len(s)
}

// PutBytes encodes a byte slice into the buffer
func (e *Encoder) PutBytes(b []byte) {
	e.ensureBufferSpace(len(b))
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

// PutCompactBytes encodes a byte slice using a compressed length format
func (e *Encoder) PutCompactBytes(b []byte) {
	e.ensureBufferSpace(5 + len(b))
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(len(b)+1))
	copy(e.b[e.offset:], b[:])
	e.offset += len(b)
}

// PutCompactArrayLen encodes the length of a compact array
func (e *Encoder) PutCompactArrayLen(l int) {
	e.ensureBufferSpace(5 + l)
	// nil arrays should give -1
	e.offset += binary.PutUvarint(e.b[e.offset:], uint64(l+1))
}

// PutLen encodes the total length of the buffer at the start
func (e *Encoder) PutLen() {
	lengthBytes := Encoding.AppendUint32([]byte{}, uint32(e.offset))
	e.b = slices.Insert(e.b, 0, lengthBytes...)
	e.offset += len(lengthBytes)
}

// PutVarIntLen encodes the total length of the buffer as a varint
func (e *Encoder) PutVarIntLen() {
	lengthBytes := binary.AppendVarint([]byte{}, int64(e.offset))
	e.b = slices.Insert(e.b, 0, lengthBytes...)
	e.offset += len(lengthBytes)
}

// EndStruct marks the end of a structure (used for tagged fields in KIP-482)
func (e *Encoder) EndStruct() {
	e.ensureBufferSpace(1)
	e.b[e.offset] = 0
	e.offset++
}

// Bytes returns the encoded data as a byte slice
func (e *Encoder) Bytes() []byte {
	return e.b[:e.offset]
}

// EncodeResponseBytes serializes encodes the response into bytes
func (e *Encoder) EncodeResponseBytes(req types.Request, response any) []byte {
	e.PutInt32(req.CorrelationID)
	e.EndStruct()
	e.Encode(response)
	e.PutLen()
	return e.Bytes()
}

// FinishAndReturn finishes the encoding and returns the final byte slice
func (e *Encoder) FinishAndReturn() []byte {
	e.EndStruct() // close struct
	e.PutLen()    // put the final length
	return e.Bytes()
}

// ParseHeader parses the header of a Kafka request
func ParseHeader(buffer []byte, connAddr string) types.Request {
	clientIDLen := Encoding.Uint16(buffer[12:])

	req := types.Request{
		Length:            Encoding.Uint32(buffer),
		RequestAPIKey:     Encoding.Uint16(buffer[4:]),
		RequestAPIVersion: Encoding.Uint16(buffer[6:]),
		CorrelationID:     Encoding.Uint32(buffer[8:]),
		ClientID:          string(buffer[14 : 14+clientIDLen]),
		ConnectionAddress: connAddr,
	}
	bodyStart := 14 + clientIDLen + 1 // + 1 for empty _tagged_fields

	// we only support empty tagged fields
	if len(buffer) < int(bodyStart) {
		log.Error("Request header bytes don't include tagged fields, you're using an old unsupported Kafka version.")
		return req
	}
	if buffer[bodyStart-1] != 0 {
		log.Panic("Request header has non empty _tagged_fields, MonKafka doesn't currently support _tagged_fields.")
	}
	req.Body = buffer[bodyStart:]
	return req
}

// Decoder is a byte slice and offset
type Decoder struct {
	b      []byte
	Offset int
}

// NewDecoder creates a new Decoder from a byte slice
func NewDecoder(b []byte) Decoder {
	return Decoder{b: b}
}

// Decode decodes the buffer into a struct using reflection
func (d *Decoder) Decode(x any) any {
	log.Debug("Decoding x of type: %v", reflect.TypeOf(x))
	v := reflect.ValueOf(x)
	k := v.Kind()
	// we expect pointers so that (CanAddr == true && canSet == True)
	// `A Value can be changed only if it is addressable and was not obtained by the use of unexported struct fields`
	// https://pkg.go.dev/reflect#Value.CanSet
	if k != reflect.Pointer {
		log.Panic("Decode expects a pointer")
	}
	v = v.Elem() // dereference
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)
		// log.Debug("Field %v Name %v Type %v Kind %v", i, field.Name, field.Type, field.Type.Kind())
		decodedValue := reflect.New(field.Type).Elem()
		if field.Type.Kind() == reflect.Slice {
			len := int(d.CompactArrayLen())
			if field.Type.Elem().Kind() == reflect.Struct { // Slice of structs
				for j := 0; j < len; j++ {
					elem := reflect.New(field.Type.Elem())                    // pointer to a new struct
					elem = reflect.ValueOf(d.Decode(elem.Interface())).Elem() // decode into new struct and dereference
					decodedValue = reflect.Append(decodedValue, elem)
				}
			} else {
				for j := 0; j < len; j++ {
					elem := reflect.New(field.Type.Elem()).Elem()
					elem = reflect.ValueOf(d.Get(elem.Interface())) // decode basic type use `Get`
					decodedValue = reflect.Append(decodedValue, elem)
				}
			}
		} else if field.Type.Kind() == reflect.Struct {
			decodedValue = reflect.ValueOf(d.Decode(decodedValue))
		} else {
			decodedValue = reflect.ValueOf(d.Get(decodedValue.Interface()))
		}
		value.Set(decodedValue)
	}
	// TODO: this assumes empty tagged fields, tags need to be handled
	d.EndStruct()
	return x
}

// Get encodes input based on its type
func (d *Decoder) Get(i any) any {
	switch c := i.(type) {
	case bool:
		return d.Bool()
	case uint8:
		return d.UInt8()
	case uint16:
		return d.UInt16()
	case uint32:
		return d.UInt32()
	case uint64:
		return d.UInt64()
	case int:
		r, _ := d.Varint()
		return r
	case uint:
		r, _ := d.Uvarint()
		return r
	case types.NonCompactString:
		return d.String()
	case string:
		return d.CompactString()
	case []byte:
		return d.CompactBytes()
	case [16]byte:
		return [16]byte(d.GetNBytes(16))
	default:
		log.Panic("Unknown type %T", c)
	}
	return nil
}

// UInt32 decodes a uint32 value from the buffer
func (d *Decoder) UInt32() uint32 {
	res := Encoding.Uint32(d.b[d.Offset:])
	d.Offset += 4
	return res
}

// UInt64 decodes a uint64 value from the buffer
func (d *Decoder) UInt64() uint64 {
	res := Encoding.Uint64(d.b[d.Offset:])
	d.Offset += 8
	return res
}

// UInt16 decodes a uint16 value from the buffer
func (d *Decoder) UInt16() uint16 {
	res := Encoding.Uint16(d.b[d.Offset:])
	d.Offset += 2
	return res
}

// UInt8 decodes a uint8 value from the buffer
func (d *Decoder) UInt8() uint8 {
	res := uint8(d.b[d.Offset])
	d.Offset++
	return res
}

// Bool decodes a boolean value from the buffer
func (d *Decoder) Bool() bool {
	res := false
	if d.b[d.Offset] > 0 {
		res = true
	}
	d.Offset++
	return res
}

// UUID decodes a 16-byte UUID from the buffer
func (d *Decoder) UUID() [16]byte {
	uuid := d.b[d.Offset : d.Offset+16]
	d.Offset += 16
	return [16]byte(uuid)
}

// String decodes a string (length + content) from the buffer
func (d *Decoder) String() string {
	stringLen := d.UInt16()
	if stringLen == 0 { // nullable string
		return ""
	}
	res := string(d.b[d.Offset : d.Offset+int(stringLen)])
	d.Offset += int(stringLen)
	return res
}

// CompactString decodes a string with a compact format
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

// Bytes decodes a byte slice from the buffer
func (d *Decoder) Bytes() []byte {
	bytesLen := int(d.CompactArrayLen())
	res := d.b[d.Offset : d.Offset+bytesLen]
	d.Offset += bytesLen
	return res
}

// CompactBytes decodes a byte slice with a compressed length format
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

// GetNBytes decodes `n` bytes from the buffer
func (d *Decoder) GetNBytes(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	res := d.b[d.Offset : d.Offset+int(n)]
	d.Offset += int(n)
	return res
}

// GetRemainingBytes reads all the remaining bytes
func (d *Decoder) GetRemainingBytes() []byte {
	res := d.b[d.Offset:]
	d.Offset += len(res)
	return res
}

// CompactArrayLen decodes the length of a compact array
func (d *Decoder) CompactArrayLen() uint64 {
	arrayLen, n := binary.Uvarint(d.b[d.Offset:])
	arrayLen--
	d.Offset += n
	return arrayLen
}

// Uvarint decodes an unsigned varint
func (d *Decoder) Uvarint() (uint64, int) {
	varint, n := binary.Uvarint(d.b[d.Offset:])
	d.Offset += n
	return varint, n
}

// Varint decodes a signed varint
func (d *Decoder) Varint() (int64, int) {
	varint, n := binary.Varint(d.b[d.Offset:])
	d.Offset += n
	log.Info("Varint %v nb bytes %v", varint, n)
	return varint, n
}

// EndStruct marks the end of a structure (used for tagged fields in KIP-482)
func (d *Decoder) EndStruct() {
	d.Offset++
}

package types

// Request is the TCP request sent by Kafka clients
type Request struct {
	Length            uint32
	RequestAPIKey     uint16
	RequestAPIVersion uint16
	CorrelationID     uint32
	ClientID          string
	ConnectionAddress string
	Body              []byte
}

// NonCompactString represents a string whose length is encoded with a UINT16 length, unlike CompactString, whose length is encoded with a varint.
// The function used to decode NonCompactString is `string()`, and the type `string` (considered a CompactString) is decoded with `compactString()`.
// This is tremendously bad!
// TODO: Create a type `CompactString` instead of using `string` to remove ambiguity.
type NonCompactString string

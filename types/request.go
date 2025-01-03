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

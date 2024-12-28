package types

type Request struct {
	Length            uint32
	RequestApiKey     uint16
	RequestApiVersion uint16
	CorrelationID     uint32
	ClientId          string
	ConnectionAddress string
	Body              []byte
}

package types

type Configuration struct {
	LogDir          string
	BrokerHost      string
	BrokerPort      uint32
	FlushIntervalMs int
}

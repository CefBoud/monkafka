package broker

import (
	"fmt"
	"io"
	"net"
	"os"

	log "github.com/CefBoud/monkafka/logging"

	"github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

// Broker represents a Kafka broker instance
type Broker struct {
	Config         types.Configuration
	ShutDownSignal chan bool
}

// NewBroker creates a new Broker instance with the provided configuration
func NewBroker(config types.Configuration) *Broker {
	return &Broker{Config: config, ShutDownSignal: make(chan bool)}
}

// Startup initializes the broker, starts the storage, loads group metadata,
// and listens for incoming connections
func (b Broker) Startup() {
	storage.Startup(b.Config, b.ShutDownSignal)
	protocol.LoadGroupMetadataState()

	// Set up a TCP listener on the specified port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		log.Error("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Info("Server is listening on port %d...\n", b.Config.BrokerPort)

	// Accept and handle incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error("Error accepting connection: %v\n", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}

// HandleConnection processes incoming requests from a client connection
func (b Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Debug("Connection established with %s\n", connectionAddr)

	for {
		// First, we read the length, then allocate a byte slice based on it.
		// ReadFull (not Read) is used to ensure the entire request is read. Partial data would result in parsing errors
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Info("failed to read request's length. Error: %v ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Error("Error reading from connection: %v\n", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)

		log.Debug("Received RequestAPIKey: %v | RequestAPIVersion: %v | CorrelationID: %v | Length: %v \n\n", protocol.APIDispatcher[req.RequestAPIKey].Name, req.RequestAPIVersion, req.CorrelationID, length)
		response := protocol.APIDispatcher[req.RequestAPIKey].Handler(req)

		_, err = conn.Write(response)
		if err != nil {
			log.Error("Error writing to connection: %v\n", err)
			break
		}
	}
	log.Debug("Connection with %s closed.\n", connectionAddr)
}

// Shutdown gracefully shuts down the broker and its components
func (b Broker) Shutdown() {
	log.Info("Broker Shutdown...")
	close(b.ShutDownSignal)
	storage.Shutdown()
}

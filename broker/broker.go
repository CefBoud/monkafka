package broker

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

type Broker struct {
	Config         types.Configuration
	ShutDownSignal chan bool
}

func NewBroker(config types.Configuration) *Broker {
	return &Broker{Config: config, ShutDownSignal: make(chan bool)}
}

func (b Broker) Startup() {
	storage.Startup(b.Config, b.ShutDownSignal)

	// Set up a TCP listener on port 9092
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		log.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Printf("Server is listening on port %d...\n", b.Config.BrokerPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}

func (b Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Printf("Connection established with %s\n", connectionAddr)

	for {
		// First, we read the length, then allocate a byte slice based on it.
		// ReadFull (not Read) is used to ensure the entire request is read. Partial data would result in parsing errors
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Print("failed to read request's length. Error: ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("Error reading from connection: %v\n", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)

		// log.Printf("Received RequestApiKey: %v | RequestApiVersion: %v | CorrelationID: %v | Length: %v \n\n", protocol.APIDispatcher[req.RequestApiKey].Name, req.RequestApiVersion, req.CorrelationID, length)
		response := protocol.APIDispatcher[req.RequestApiKey].Handler(req)

		_, err = conn.Write(response)
		if err != nil {
			log.Printf("Error writing to connection: %v\n", err)
			break
		}
	}
	log.Printf("Connection with %s closed.\n", connectionAddr)
}

func (b Broker) Shutdown() {
	log.Println("Broker Shutdown...")
	close(b.ShutDownSignal)
	storage.Shutdown()
}

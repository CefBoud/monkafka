package main

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/storage"
)

var LogDir = filepath.Join("/tmp", "MonKafka")

func handleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Printf("Connection established with %s\n", connectionAddr)

	for {

		// First, we read the length, then allocate a byte slice based on it.
		// ReadFull (not Read) is used to ensure the entire request is read. Partial data would result in parsing errors
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Print("failed to read request length. Error: ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		// Read incoming data
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("Error reading from connection: %v\n", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)
		log.Printf("Received RequestApiKey: %v | RequestApiVersion: %v | CorrelationID: %v | Length: %v \n\n", protocol.APIDispatcher[req.RequestApiKey].Name, req.RequestApiVersion, req.CorrelationID, length)

		// handle request based on its Api key
		response := protocol.APIDispatcher[req.RequestApiKey].Handler(req)

		_, err = conn.Write(response)
		// log.Printf("sent back %v bytes \n\n", N)
		if err != nil {
			log.Printf("Error writing to connection: %v\n", err)
			break
		}
	}
	log.Printf("Connection with %s closed.\n", connectionAddr)
}

func main() {

	// load state
	_, err := storage.LoadTopicsState(LogDir) //topicState
	if err != nil {
		log.Printf("Error loading TopicsState: %v\n", err)
		os.Exit(1)
	}
	// Set up a TCP listener on port 9092
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Println("Server is listening on port 9092...")

	for {
		// Accept a new client connection
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

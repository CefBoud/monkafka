package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/CefBoud/monkafka/protocol"
	"github.com/CefBoud/monkafka/serde"
	"github.com/CefBoud/monkafka/storage"
	"github.com/CefBoud/monkafka/types"
)

var Config = types.Configuration{
	LogDir:          filepath.Join(os.TempDir(), "MonKafka"),
	BrokerHost:      "localhost",
	BrokerPort:      9092,
	FlushIntervalMs: 5000,
}

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
	shutdown := make(chan bool)

	storage.Startup(Config, shutdown)

	// Set up a TCP listener on port 9092
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", Config.BrokerPort))
	if err != nil {
		log.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Printf("Server is listening on port %d...\n", Config.BrokerPort)

	signalChannel := make(chan os.Signal, 1)

	// Notify the channel when an interrupt (Ctrl+C) or termination signal is received.
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signalChannel
		shutdown <- true
		log.Printf("\nReceived signal: %s. Shutting down...\n", sig)
		log.Println("Performing cleanup...")
		storage.GracefulShutdown()
		// Graceful exit
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

package logging

import (
	"fmt"
	"log"
	"os"
)

// logging levels
const (
	DEBUG = "DEBUG"
	INFO  = "INFO"
	WARN  = "WARN"
	ERROR = "ERROR"
)

// LogLevel defines the current logging level (default is INFO)
var LogLevel = "INFO"

// SetLogLevel sets the log level for filtering logs
func SetLogLevel(logLevel string) {
	LogLevel = logLevel
}

// Log writes a log message at a specified level, formatted with optional arguments
func Log(level, message string, a ...any) {
	levels := map[string]int{
		DEBUG: 1,
		INFO:  2,
		WARN:  3,
		ERROR: 4,
	}

	// Log only if the message level is greater than or equal to the current LogLevel
	if levels[level] >= levels[LogLevel] {
		log.SetOutput(os.Stdout)
		log.Printf("[%s] %s\n", level, fmt.Sprintf(message, a...))
	}
}

// Debug logs a message at DEBUG level
func Debug(message string, a ...any) {
	Log(DEBUG, message, a...)
}

// Info logs a message at INFO level
func Info(message string, a ...any) {
	Log(INFO, message, a...)
}

// Warn logs a message at WARN level
func Warn(message string, a ...any) {
	Log(WARN, message, a...)
}

// Error logs a message at ERROR level
func Error(message string, a ...any) {
	Log(ERROR, message, a...)
}

// Panic exists with a panic
func Panic(message string, a ...any) {
	panic(fmt.Sprintf(message, a...))
}

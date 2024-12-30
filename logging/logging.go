package logging

import (
	"fmt"
	"log"
	"os"
)

const (
	DEBUG = "DEBUG"
	INFO  = "INFO"
	WARN  = "WARN"
	ERROR = "ERROR"
)

// default
var LogLevel = "INFO"

func SetLogLevel(logLevel string) {
	LogLevel = logLevel
}

func Log(level, message string, a ...any) {
	levels := map[string]int{
		DEBUG: 1,
		INFO:  2,
		WARN:  3,
		ERROR: 4,
	}

	if levels[level] >= levels[LogLevel] {
		log.SetOutput(os.Stdout)
		log.Printf("[%s] %s\n", level, fmt.Sprintf(message, a...))
	}
}

func Debug(message string, a ...any) {
	Log(DEBUG, message, a...)
}
func Info(message string, a ...any) {
	Log(INFO, message, a...)
}
func Warn(message string, a ...any) {
	Log(WARN, message, a...)
}
func Error(message string, a ...any) {
	Log(ERROR, message, a...)
}

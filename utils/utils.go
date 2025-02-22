package utils

import (
	"os"
	"path/filepath"
	"time"
)

// NowAsUnixMilli returns current time in ms
func NowAsUnixMilli() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}

// EnsurePath is used to make sure a path exists
func EnsurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

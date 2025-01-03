package utils

import "time"

// NowAsUnixMilli returns current time in ms
func NowAsUnixMilli() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}

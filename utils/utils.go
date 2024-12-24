package utils

import "time"

func NowAsUnixMilli() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}

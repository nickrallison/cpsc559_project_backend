package util

import "time"

func MakeTimestamp() int64 {
	return time.Now().UnixNano() / 1e6
}

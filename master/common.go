package master

import (
	"time"

	"github.com/abcdabcd987/llgfs"
)

type lessee struct {
	primary     llgfs.ServerAddress
	expire      time.Time
	secondaries []llgfs.ServerAddress
}

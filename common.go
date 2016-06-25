package llgfs

import "time"

type Path string
type ServerAddress string
type ChunkIndex int64
type ChunkHandle int64
type DataBufferId int64

const (
	LeaseExpire       = 1 * time.Minute
	HeartbeatInterval = 100 * time.Millisecond
)

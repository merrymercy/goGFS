package llgfs

import "time"

type Path string
type ServerAddress string
type ChunkIndex int64
type ChunkHandle int64
type DataBufferId int64

type ChunkInfo struct {
	handle   ChunkHandle
	replicas []ServerAddress
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

const (
	LeaseExpire       = 1 * time.Minute
	HeartbeatInterval = 100 * time.Millisecond
)

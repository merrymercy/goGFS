package gfs

import "time"

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type DataBufferID int64

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
	LeaseExpire        = 1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	MaxChunkSize       = 512 << 10 //64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DefaultNumReplicas = 3
)

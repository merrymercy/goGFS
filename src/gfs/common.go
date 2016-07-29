package gfs

import "time"

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64

type DataBufferID struct {
    Handle    ChunkHandle
    TimeStamp int
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int
const (
    MutationWrite  = iota
    MutationAppend
)

type Error struct {
    Code    int
    Message string
}
// error code constants
const (
    APPEND_EXCEED_CHUNKSIZE = iota
    WRITE_EXCEED_CHUNKSIZE
)

func (e Error) Error() string {
    return e.Message
}

// system config
const (
	LeaseExpire        = 1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	MaxChunkSize       = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DefaultNumReplicas = 3

    DownloadBufferExpire = 2 * time.Minute
    DownloadBufferTick   = 10 * time.Second
)

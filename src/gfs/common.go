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
    MutationPad
)

type ErrorCode int
type Error struct {
    Code    ErrorCode
    Err     string
}
// error code constants
const (
    UnknownError          = iota
    AppendExceedChunkSize
    WriteExceedChunkSize
)

func (e Error) Error() string {
    return e.Err
}

// system config
const (
	LeaseExpire        = 1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	MaxChunkSize       = 1 << 6 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DefaultNumReplicas = 3

    DownloadBufferExpire = 2 * time.Minute
    DownloadBufferTick   = 10 * time.Second
)

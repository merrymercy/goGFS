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
	MutationWrite = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

const (
	Success = iota
	UnknownError
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

// system config
const (
	LeaseExpire        = 2 * time.Second //1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond //
	ServerTimeout      = 1 * time.Second

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second
)

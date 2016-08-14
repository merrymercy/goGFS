package gfs

import "time"

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64
type Checksum int64

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type Lease struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type PersistentChunkInfo struct {
	Handle   ChunkHandle
	Length   Offset
	Version  ChunkVersion
	Checksum Checksum
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
	Timeout
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

var (
	Debug int
)

// system config
const (
	// chunk
	LeaseExpire        = 3 * time.Second //1 * time.Minute
	DefaultNumReplicas = 3
	MinimumNumReplicas = 2
	MaxChunkSize       = 32 << 20 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DeletedFilePrefix  = "__del__"

	// master
	ServerCheckInterval = 400 * time.Millisecond //
	MasterStoreInterval = 30 * time.Hour         // 30 * time.Minute
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval    = 200 * time.Millisecond
	MutationWaitTimeout  = 4 * time.Second
	ServerStoreInterval  = 40 * time.Hour // 30 * time.Minute
	GarbageCollectionInt = 30 * time.Hour // 1 * time.Day
	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 30 * time.Second

	// client
	ClientTryTimeout = 2*LeaseExpire + 3*ServerTimeout
	LeaseBufferTick  = 500 * time.Millisecond
)

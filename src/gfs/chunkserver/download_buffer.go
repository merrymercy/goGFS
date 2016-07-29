package chunkserver

import (
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs/gfs"
)

type downloadItem struct {
	data   []byte
	expire time.Time
}

type downloadBuffer struct {
	sync.RWMutex
	buffer map[gfs.DataBufferID]downloadItem
	expire time.Duration
	tick   time.Duration
}

// newDownloadBuffer returns a downloadBuffer. Default expire time is expire.
// The downloadBuffer will cleanup expired items every tick.
func newDownloadBuffer(expire, tick time.Duration) *downloadBuffer {
	buf := &downloadBuffer{expire: expire, tick: tick}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			buf.Lock()
			now := time.Now()
			for key, item := range buf.buffer {
				if item.expire.Before(now) {
					delete(buf.buffer, key)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *downloadBuffer) Set(id gfs.DataBufferID, data []byte) {
	buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
}

func (buf *downloadBuffer) Get(id gfs.DataBufferID) ([]byte, bool) {
	item, ok := buf.buffer[id]
	if !ok {
		return nil, ok
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, ok
}

func (buf *downloadBuffer) Delete(id gfs.DataBufferID) {
	delete(buf.buffer, id)
}

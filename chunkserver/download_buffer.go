package chunkserver

import (
	"sync"
	"time"

	"github.com/abcdabcd987/llgfs"
)

type downloadItem struct {
	data   []byte
	expire time.Time
}

type downloadBuffer struct {
	lock   sync.RWMutex
	buffer map[llgfs.DataBufferId]downloadItem
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
			buf.lock.Lock()
			now := time.Now()
			for key, item := range buf.buffer {
				if item.expire.Before(now) {
					delete(buf.buffer, key)
				}
			}
			buf.lock.Unlock()
		}
	}()

	return buf
}

func (buf *downloadBuffer) Set(id llgfs.DataBufferId, data []byte) {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
}

func (buf *downloadBuffer) Get(id llgfs.DataBufferId) ([]byte, bool) {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	item, ok := buf.buffer[id]
	if !ok {
		return nil, ok
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, ok
}

func (buf *downloadBuffer) Delete(id llgfs.DataBufferId) {
	buf.lock.Lock()
	defer buf.lock.Unlock()
	delete(buf.buffer, id)
}

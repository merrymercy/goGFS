package client

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"
	//log "github.com/Sirupsen/logrus"
)

type leaseBuffer struct {
	sync.RWMutex
	master gfs.ServerAddress
	buffer map[gfs.ChunkHandle]*gfs.Lease
	tick   time.Duration
}

// newLeaseBuffer returns a leaseBuffer.
// The downloadBuffer will cleanup expired items every tick.
func newLeaseBuffer(ms gfs.ServerAddress, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer: make(map[gfs.ChunkHandle]*gfs.Lease),
		tick:   tick,
		master: ms,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.Expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *leaseBuffer) Get(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]

	if !ok { // ask master to send one
		var l gfs.GetPrimaryAndSecondariesReply
		err := util.Call(buf.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{handle}, &l)
		if err != nil {
			return nil, err
		}

		lease = &gfs.Lease{l.Primary, l.Expire, l.Secondaries}
		buf.buffer[handle] = lease
		return lease, nil
	}
	// extend lease (it is the work of chunk server)
	/*
	   go func() {
	       var r gfs.ExtendLeaseReply
	       util.Call(buf.master, "Master.RPCExtendLease", gfs.ExtendLeaseArg{handle, lease.Primary}, &r)
	       lease.Expire = r.Expire
	   }()
	*/
	return lease, nil
}

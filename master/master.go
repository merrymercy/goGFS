package master

import (
	"log"
	"net"
	"net/rpc"
)

import "github.com/abcdabcd987/llgfs"

// Master Server struct
type Master struct {
	address  llgfs.ServerAddress // master server address
	l        net.Listener
	shutdown chan bool

	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and return the pointer to it.
func NewAndServe(address llgfs.ServerAddress) *Master {
	m := &Master{
		address: address,
		cm:      new(chunkManager),
		csm:     new(chunkServerManager),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	m.l = l

	// RPC Handler
	go func() {
	loop:
		for {
			select {
			case <-m.shutdown:
				break loop
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
			}
		}
	}()

	return m
}

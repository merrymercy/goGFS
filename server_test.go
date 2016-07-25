package chunkserver

import (
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/chunkserver"
	"github.com/abcdabcd987/llgfs/gfs/client"
	"github.com/abcdabcd987/llgfs/gfs/master"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
)

var (
	m                       *master.Master
	cs1, cs2, cs3, cs4, cs5 *chunkserver.ChunkServer
	c                       *client.Client
)

func TestRPCGetChunkHandle(t *testing.T) {
	var r1, r2 gfs.GetChunkHandleReply
	path := "/data/test.txt"
	err := m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r2)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Error("got different handle: %v and %v", r1.Handle, r2.Handle)
	}
}

func TestMain(m *testing.M) {
	// create temporary directory
	root, err := ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", e)
	}
	defer os.RemoveAll(root)

	// run master and chunkservers
	m = master.NewAndServe(":7777")
	cs1 = chunkserver.NewAndServe(":10001", ":7777", path.Join(root, "cs1"))
	cs2 = chunkserver.NewAndServe(":10002", ":7777", path.Join(root, "cs2"))
	cs3 = chunkserver.NewAndServe(":10003", ":7777", path.Join(root, "cs3"))
	cs4 = chunkserver.NewAndServe(":10004", ":7777", path.Join(root, "cs4"))
	cs5 = chunkserver.NewAndServe(":10005", ":7777", path.Join(root, "cs5"))
	defer cs5.Shutdown()
	defer cs4.Shutdown()
	defer cs3.Shutdown()
	defer cs2.Shutdown()
	defer cs1.Shutdown()
	defer m.Shutdown()

	// init client
	c = client.NewClient(":7777")

	// run tests
	os.Exit(m.Run())
}

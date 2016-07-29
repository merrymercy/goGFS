package main

import (
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/chunkserver"
	"github.com/abcdabcd987/llgfs/gfs/client"
	"github.com/abcdabcd987/llgfs/gfs/master"

	"io/ioutil"
	log "github.com/Sirupsen/logrus"
	"os"
	"path"
	"testing"
    "time"
)

var (
	mr                      *master.Master
	cs1, cs2, cs3, cs4, cs5 *chunkserver.ChunkServer
	c                       *client.Client
)

func TestRPCGetChunkHandle(t *testing.T) {
	var r1, r2 gfs.GetChunkHandleReply
	path := gfs.Path("/data/test.txt")
	err := mr.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	err = mr.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r2)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Error("got different handle: %v and %v", r1.Handle, r2.Handle)
	}
}

const (
    testfile1 = gfs.Path("/first.txt")
)

func TestCreateFile(t *testing.T) {
    err := c.Create(gfs.Path("/first.txt"))
    if err != nil { t.Error(err) }
    time.Sleep(25 * time.Second)
}

func TestAppendFile(t *testing.T) {
    err := c.Create(testfile1)
    if err != nil { t.Error(err) }
    _, err = c.Append(testfile1, []byte("Hello World!"))
    if err != nil { t.Error(err) }

    time.Sleep(25 * time.Second)
}

func TestNull(t *testing.T) {
    log.Println("Haa")
    time.Sleep(1500 * time.Millisecond)
}

func TestMain(m *testing.M) {
	// create temporary directory
	root, err := ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	// run master and chunkservers
	mr = master.NewAndServe(":7777")
	cs1 = chunkserver.NewAndServe(":10001", ":7777", path.Join(root, "cs1"))
	cs2 = chunkserver.NewAndServe(":10002", ":7777", path.Join(root, "cs2"))
	cs3 = chunkserver.NewAndServe(":10003", ":7777", path.Join(root, "cs3"))
	cs4 = chunkserver.NewAndServe(":10004", ":7777", path.Join(root, "cs4"))
	cs5 = chunkserver.NewAndServe(":10005", ":7777", path.Join(root, "cs5"))

	// init client
	c = client.NewClient(":7777")

    time.Sleep(100 * time.Millisecond)
    ret := m.Run()

    // shutdown
	cs5.Shutdown()
	cs4.Shutdown()
	cs3.Shutdown()
	cs2.Shutdown()
	cs1.Shutdown()
	mr.Shutdown()
	//os.RemoveAll(root)

	// run tests
	os.Exit(ret)
}

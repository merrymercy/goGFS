package main

import (
	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
	"reflect"

	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

var (
	m                       *master.Master
	cs1, cs2, cs3, cs4, cs5 *chunkserver.ChunkServer
	c                       *client.Client
)

const (
	N = 10
)

func errorAll(ch chan error, n int, t *testing.T) {
	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Error(err)
		}
	}
}

func TestCreateFile(t *testing.T) {
	err := m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}
}

func TestRPCGetChunkHandle(t *testing.T) {
	var r1, r2 gfs.GetChunkHandleReply
	path := gfs.Path("/test1.txt")
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

	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 2}, &r2)
	if err == nil {
		t.Error("discontinuous chunk should not be created")
	}
}

func TestWriteChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	ch := make(chan error, N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	for i := 0; i < N; i++ {
		go func(x int) {
			ch <- c.WriteChunk(r1.Handle, gfs.Offset(x*2), []byte(fmt.Sprintf("%2d", x)))
		}(i)
	}
	errorAll(ch, N+2, t)
}

func TestReadChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	ch := make(chan error, N+1)
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	for i := 0; i < N; i++ {
		go func(x int) {
			buf := make([]byte, 2)
			n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*2), buf)
			ch <- err
			expected := []byte(fmt.Sprintf("%2d", x))
			if n != 2 {
				t.Error("should read exactly 2 bytes but", n, "instead")
			} else if !reflect.DeepEqual(expected, buf) {
				t.Error("expected (", expected, ") != buf (", buf, ")")
			}
		}(i)
	}
	errorAll(ch, N+1, t)
}

func TestReplicaEquality(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	var r2 gfs.ReadChunkReply
	var data [][]byte
	p := gfs.Path("/TestWriteChunk.txt")
	m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	args := gfs.ReadChunkArg{r1.Handle, 0, 2 * N}

	if err := cs1.RPCReadChunk(args, &r2); err == nil {
		data = append(data, r2.Data)
	}
	if err := cs2.RPCReadChunk(args, &r2); err == nil {
		data = append(data, r2.Data)
	}
	if err := cs3.RPCReadChunk(args, &r2); err == nil {
		data = append(data, r2.Data)
	}
	if err := cs4.RPCReadChunk(args, &r2); err == nil {
		data = append(data, r2.Data)
	}
	if err := cs5.RPCReadChunk(args, &r2); err == nil {
		data = append(data, r2.Data)
	}

	if len(data) != gfs.DefaultNumReplicas {
		t.Error("expect", gfs.DefaultNumReplicas, "replicas, got only", len(data))
	}
	for i := 1; i < len(data); i++ {
		if !reflect.DeepEqual(data[0], data[i]) {
			t.Error("replicas are different. ", data[0], "vs", data[i])
		}
	}
}

func TestAppendChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestAppendChunk.txt")
	ch := make(chan error, 2*N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	done := make(chan bool, N)
	expected := make(map[int][]byte)

	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
	}
	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.AppendChunk(r1.Handle, expected[x])
			ch <- err
			done <- true
		}(i)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	for x := 0; x < N; x++ {
		buf := make([]byte, 2)
		n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*2), buf)
		ch <- err
		if n != 2 {
			t.Error("should read exactly 2 bytes but", n, "instead")
		}
		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		if key == -1 {
			t.Error("incorrect data", buf)
		} else {
			delete(expected, key)
		}
	}
	if len(expected) != 0 {
		t.Error("incorrect data")
	}
	errorAll(ch, 2*N+2, t)
}

func TestMain(tm *testing.M) {
	// create temporary directory
	root, err := ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

    log.SetLevel(log.FatalLevel)

	os.Mkdir(path.Join(root, "m"), 0755)
	os.Mkdir(path.Join(root, "cs1"), 0755)
	os.Mkdir(path.Join(root, "cs2"), 0755)
	os.Mkdir(path.Join(root, "cs3"), 0755)
	os.Mkdir(path.Join(root, "cs4"), 0755)
	os.Mkdir(path.Join(root, "cs5"), 0755)

	// run master and chunkservers
	m = master.NewAndServe(":7777", path.Join(root, "m"))
	cs1 = chunkserver.NewAndServe(":10001", ":7777", path.Join(root, "cs1"))
	cs2 = chunkserver.NewAndServe(":10002", ":7777", path.Join(root, "cs2"))
	cs3 = chunkserver.NewAndServe(":10003", ":7777", path.Join(root, "cs3"))
	cs4 = chunkserver.NewAndServe(":10004", ":7777", path.Join(root, "cs4"))
	cs5 = chunkserver.NewAndServe(":10005", ":7777", path.Join(root, "cs5"))

	// init client
	c = client.NewClient(":7777")

	time.Sleep(300 * time.Millisecond)

	ret := tm.Run()

	// shutdown
	cs5.Shutdown()
	cs4.Shutdown()
	cs3.Shutdown()
	cs2.Shutdown()
	cs1.Shutdown()
	m.Shutdown()
	os.RemoveAll(root)

	// run tests
	os.Exit(ret)
}


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
    "sync"
	"testing"
	"time"
)

var (
	m                       *master.Master
	cs1, cs2, cs3, cs4, cs5 *chunkserver.ChunkServer
	c                       *client.Client
    root                    string           // root of tmp file path
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
    close(ch)
}

/*
 *  TEST WITHOUT CLIENT (use RPC call directly)
 */
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

func checkReplicas(handle gfs.ChunkHandle, length int, t *testing.T) (int) {
	var r gfs.ReadChunkReply
    var data [][]byte

    css := []*chunkserver.ChunkServer{cs1, cs2, cs3, cs4, cs5}
    args := gfs.ReadChunkArg{handle, 0, length}
    for _, cs := range css {
        if err := cs.RPCReadChunk(args, &r); err == nil {
            data = append(data, r.Data)
            //fmt.Println("found in ", cs)
        }
    }

	for i := 1; i < len(data); i++ {
		if !reflect.DeepEqual(data[0], data[i]) {
			t.Error("replicas are different. ", data[0], "vs", data[i])
		}
	}

    return len(data)
}

func TestReplicaEquality(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	var data [][]byte
	p := gfs.Path("/TestWriteChunk.txt")
	m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)

    n := checkReplicas(r1.Handle, N * 2, t)
	if n != gfs.DefaultNumReplicas {
		t.Error("expect", gfs.DefaultNumReplicas, "replicas, got only", len(data))
	}
}

func TestAppendChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestAppendChunk.txt")
	ch := make(chan error, 2*N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	expected := make(map[int][]byte)

	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
	}
    var wg sync.WaitGroup
    wg.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.AppendChunk(r1.Handle, expected[x])
			ch <- err
            wg.Done()
		}(i)
	}
    wg.Wait()

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


/*
 *  TEST WITH CLIENT (use client API)
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
    p := gfs.Path("/appendover.txt")

    ch := make(chan error, 6)
    ch <- c.Create(p)

    bound := gfs.MaxAppendSize - 1
    buf := make([]byte, bound)
    for i := 0; i < bound; i++ {
        buf[i] = byte(i % 26 + 'a')
    }

    for i := 0; i < 4; i++ {
        _, err := c.Append(p, buf)
        ch <- err
    }

    buf = buf[:5]
    offset, err := c.Append(p, buf)
    ch <- err
    if offset != gfs.MaxChunkSize { // i.e. 0 at next chunk
        t.Error("data should be appended to the beginning of next chunk")
    }

    errorAll(ch, 6, t)
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
    p := gfs.Path("/bigData.txt")

    ch := make(chan error, 4)
    ch <- c.Create(p)

    size := gfs.MaxChunkSize * 3
    expected := make([]byte, size)
    for i := 0; i < size; i++ {
        expected[i] = byte(i % 26 + 'a')
    }

    // write large data
    ch <- c.Write(p, gfs.MaxChunkSize / 2, expected)

    // read 
    buf := make([]byte, size)
    n, err := c.Read(p, gfs.MaxChunkSize / 2, buf)
    ch <- err

    if n != size {
        t.Error("read counter is wrong")
    }
    if !reflect.DeepEqual(expected, buf) {
        t.Error("read wrong data")
    }

    // read at EOF
    n, err = c.Read(p, gfs.MaxChunkSize / 2 + gfs.Offset(size), buf)
    if err == nil {
        t.Error("an error should be returned if read at EOF")
    }

    // append 
    var offset gfs.Offset
    buf = buf[:gfs.MaxAppendSize - 1]
    offset, err = c.Append(p, buf)
    if offset != gfs.MaxChunkSize / 2 + gfs.Offset(size) {
        t.Error("append in wrong offset")
    }
    ch <- err

    errorAll(ch, 4, t)
}

// Shutdown two chunk servers during appending
func TestShutdownInAppend(t *testing.T) {
    p := gfs.Path("/shutdown.txt")

    ch := make(chan error, 2 * N + 1)
    ch <- c.Create(p)
    expected := make(map[int][]byte)
    for i := 0; i < N; i++ {
        expected[i] = []byte(fmt.Sprintf("%2d", i))
    }

    var wg sync.WaitGroup
    wg.Add(N)
    for i := 0; i < N; i++ {
        go func(x int) {
            _, err := c.Append(p, expected[x])
            ch <- err
            wg.Done()
        }(i)
    }

    // shutdown
    cs2.Shutdown()
    cs3.Shutdown()

    wg.Wait()

    // only check replicas equality
    // (the number of replicas will be check in following re-replication test)
	var r1 gfs.GetChunkHandleReply
	m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
    n := checkReplicas(r1.Handle, N * 2, t)
    if n < 1 {
        t.Error("lose data in shutdown")
    }

    // check correctness
    for x := 0; x < N; x++ {
        buf := make([]byte, 2)
        n, err := c.Read(p, gfs.Offset(x*2), buf)
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

	errorAll(ch, 2*N+1, t)

    // restart
	cs2 = chunkserver.NewAndServe(":10002", ":7777", path.Join(root, "cs2"))
	cs3 = chunkserver.NewAndServe(":10003", ":7777", path.Join(root, "cs3"))
}

// Shutdown all servers in turn. You should perform re-Replication well
func TestReReplication(t *testing.T) {
    p := gfs.Path("/re-replication.txt")

    ch := make(chan error, 2)
    ch <- c.Create(p)

    c.Append(p, []byte("Dangerous"))

    fmt.Println("###### Mr. Disaster is coming...")
    time.Sleep(gfs.LeaseExpire)

    cs1.Shutdown()
    cs2.Shutdown()
    time.Sleep(gfs.ServerTimeout * 2)

	cs1 = chunkserver.NewAndServe(":10001", ":7777", path.Join(root, "cs1"))
	cs2 = chunkserver.NewAndServe(":10002", ":7777", path.Join(root, "cs2"))

    cs3.Shutdown()
    time.Sleep(gfs.ServerTimeout * 2)

    cs4.Shutdown()
    time.Sleep(gfs.ServerTimeout * 2)

	cs3 = chunkserver.NewAndServe(":10003", ":7777", path.Join(root, "cs3"))
	cs4 = chunkserver.NewAndServe(":10004", ":7777", path.Join(root, "cs4"))
    time.Sleep(gfs.ServerTimeout)

    cs5.Shutdown()
    time.Sleep(gfs.ServerTimeout * 2)

	cs5 = chunkserver.NewAndServe(":10005", ":7777", path.Join(root, "cs5"))
    time.Sleep(gfs.ServerTimeout)

    // check equality and number of replicas
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
    n := checkReplicas(r1.Handle, N * 2, t)

    if n < gfs.MinimumNumReplicas {
        t.Errorf("Cannot perform replicas promptly, only get %v replicas", n)
    } else {
        fmt.Printf("###### Well done, you save %v replicas during disaster\n", n)
    }

    errorAll(ch, 2, t)
}

func TestMain(tm *testing.M) {
	// create temporary directory
    var err error
	root, err = ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

    log.SetLevel(log.WarnLevel)

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
    cs1.Shutdown()
    cs2.Shutdown()
    cs3.Shutdown()
    cs4.Shutdown()
    cs5.Shutdown()
	m.Shutdown()
	os.RemoveAll(root)

	// run tests
	os.Exit(ret)
}


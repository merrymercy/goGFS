package main

import (
	"github.com/abcdabcd987/llgfs/gfs"
	"github.com/abcdabcd987/llgfs/gfs/chunkserver"
	"github.com/abcdabcd987/llgfs/gfs/client"
	"github.com/abcdabcd987/llgfs/gfs/master"

    "fmt"
    "io"
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
	c1, c2                  *client.Client
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
    testfile2 = gfs.Path("/second.txt")
    n         = 100
)

func PrintFinalFile() {
    time.Sleep(gfs.LeaseExpire * 2)
    cs1.PrintSelf()
    cs2.PrintSelf()
    cs3.PrintSelf()
    cs4.PrintSelf()
    cs5.PrintSelf()
}

func TestCreateFile(t *testing.T) {
    err := c1.Create(gfs.Path("/first.txt"))
    if err != nil { t.Error(err) }
    time.Sleep(25 * time.Second)
}

func TestReReplication(t *testing.T) {
    err := c1.Create(testfile1)
    if err != nil { t.Error(err) }

    c1.Append(testfile1, []byte("Hello"))

    cs1.Shutdown()
    cs2.Shutdown()

    PrintFinalFile()
}

func TestAppendFile(t *testing.T) {
    err := c1.Create(testfile2)
    if err != nil { t.Error(err) }
    err = c2.Create(testfile1)
    if err != nil { t.Error(err) }
    ch := make(chan error)

    for i := 0; i < n; i++ {
        go func(x int) {
            _, err = c1.Append(testfile1, []byte(fmt.Sprintf("%3d", x)))
            ch <- err
        }(i)
    }
    for i := 0; i < n; i++ {
        go func(x int) {
            _, err = c2.Append(testfile2, []byte(fmt.Sprintf("-%3d-", x + 20)))
            ch <- err
        }(i)
    }

    cs2.Shutdown()
    cs3.Shutdown()

    for i := 0; i < 1 * n; i++ {
        if err := <-ch; err != nil {
            t.Error(err)
        }
    }

    time.Sleep(5000 * time.Millisecond)
    PrintFinalFile()
}

func TestWriteFile(t *testing.T) {
    err := c1.Create(testfile2)
    if err != nil { t.Error(err) }
    err = c2.Create(testfile1)
    if err != nil { t.Error(err) }

    ch := make(chan error, n)
    for i := 0; i < n; i++ {
        go func(x int) {
            ch <- c1.Write(testfile1, gfs.Offset(x * 2), []byte(fmt.Sprintf("%2d", x)))
        }(i)
    }
    for i := 0; i < n; i++ {
        go func(x int) {
            ch <- c2.Write(testfile2, gfs.Offset(x * 2), []byte(fmt.Sprintf("%2d", x + 20)))
        }(i)
    }

    cs2.Shutdown()
    cs3.Shutdown()

    for i := 0; i < 2 * n; i++ {
        if err := <-ch; err != nil {
            t.Error(err)
        }
    }

    PrintFinalFile()
}

func TestReadFile(t * testing.T) {
    err := c1.Create(testfile2)
    if err != nil { t.Error(err) }
    err = c2.Create(testfile1)
    if err != nil { t.Error(err) }

    // write file
    c1.Write(testfile1, gfs.Offset(0), []byte("1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18.19.20.21.22"))
    c2.Write(testfile2, gfs.Offset(0), []byte("A.B.C.D.E.F.G.H.I.J.K.L.M.N.O.P.Q.R.S.T.U.V.W.X.Y.Z"))

    cs2.Shutdown()
    cs3.Shutdown()

    // read file
    ch := make(chan error, n)
    for i := 0; i < n; i++ {
        go func(x int) {
            buf := make([]byte, 64)
            n, err := c1.Read(testfile1, gfs.Offset(x * 2), buf)
            log.Infof("read %v at %v : %v (ret : %v, %v)", testfile1, x * 2, string(buf), n, err)
            ch <- err
        }(i)
    }
    for i := 0; i < n; i++ {
        go func(x int) {
            buf := make([]byte, 64)
            n, err := c2.Read(testfile2, gfs.Offset(x * 2), buf)
            log.Infof("read %v at %v : %v (ret: %v, %v)", testfile2, x * 2, string(buf), n, err)
            ch <- err
        }(i)
    }


    for i := 0; i < 2 * n; i++ {
        if err := <-ch; err != nil && err != io.EOF {
            t.Error(err)
        }
    }

    PrintFinalFile()
}

func TestMixFile(t *testing.T) {
    err := c1.Create(testfile2)
    if err != nil { t.Error(err) }
    err = c2.Create(testfile1)
    if err != nil { t.Error(err) }
    ch := make(chan error)

    for i := 0; i < n; i++ {
        go func(x int) {
            _, err = c1.Append(testfile1, []byte(fmt.Sprintf("-%d-", x)))
            ch <- err
        }(i)
    }

    for i := 0; i < n; i++ {
        go func(x int) {
            ch <- c1.Write(testfile2, gfs.Offset(2), []byte(fmt.Sprintf("hh")))
        }(i)
    }

    cs2.Shutdown()
    cs3.Shutdown()

    for i := 0; i < 2 * n; i++ {
        if err := <-ch; err != nil {
            t.Error(err)
        }
    }

    PrintFinalFile()
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
	c1 = client.NewClient(":7777")
	c2 = client.NewClient(":7777")

    time.Sleep(300 * time.Millisecond)
    ret := m.Run()

    // shutdown
	/*cs5.Shutdown()
	cs4.Shutdown()
	cs3.Shutdown()
	cs2.Shutdown()
	cs1.Shutdown()
	mr.Shutdown()*/
	os.RemoveAll(root)

	// run tests
	os.Exit(ret)
}

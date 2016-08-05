package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
    "math/rand"
	"time"

	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
	"os"
)

func runTestClient() {
	c := client.NewClient(gfs.ServerAddress(os.Args[2]))
    rand.Seed(time.Now().UTC().UnixNano())

	for {
        ct := rand.Int63()
		filename := gfs.Path(fmt.Sprintf("/haha%v.txt", ct))
        err := c.Create(filename)
        if err != nil {
            log.Warning(err)
        }
        for i := 0; i < 100; i++ {
            _, err =c.Append(filename, []byte("Hello"))
            if err != nil {
                log.Warning(err)
            }
        }
		time.Sleep(time.Second)
	}
}

func runMaster() {
	if len(os.Args) < 4 {
		printUsage()
		return
	}
	addr := gfs.ServerAddress(os.Args[2])
	master.NewAndServe(addr, os.Args[3])

	ch := make(chan bool)
	<-ch
}

func runChunkServer() {
	if len(os.Args) < 5 {
		printUsage()
		return
	}
	addr := gfs.ServerAddress(os.Args[2])
	serverRoot := os.Args[3]
	masterAddr := gfs.ServerAddress(os.Args[4])
	chunkserver.NewAndServe(addr, masterAddr, serverRoot)

	ch := make(chan bool)
	<-ch
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  gfs master <addr> <root path>")
	fmt.Println("  gfs chunkserver <addr> <root path> <master addr>")
	fmt.Println("  gfs client <master addr>")
	fmt.Println()
}

func main() {
	log.SetLevel(log.DebugLevel)
	if len(os.Args) < 2 {
		printUsage()
		return
	}
	switch os.Args[1] {
	case "master":
		runMaster()
	case "chunkserver":
		runChunkServer()
	case "client":
		runTestClient()
	default:
		printUsage()
	}
}

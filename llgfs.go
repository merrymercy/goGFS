package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

	"gfs"
	"gfs/chunkserver"
	"gfs/master"
	"os"
)

func runMaster() {
	if len(os.Args) < 3 {
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
	default:
		printUsage()
	}
}

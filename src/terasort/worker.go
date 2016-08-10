package terasort

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

    "sort"
    "io"
    "os"
	"net"
	"net/rpc"
    "strings"
	//"sync"
)

type Worker struct {
	address string
	master  string
	rootDir string

    sample   []string
    config   map[string]string

	l        net.Listener
	shutdown chan struct{}
	dead     bool
}

func NewWorker(address, master string, rootDir string) *Worker {
	wk := &Worker{
		address:  address,
		master:   master,
		rootDir:  rootDir,

        config: make(map[string]string),
		shutdown: make(chan struct{}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	l, e := net.Listen("tcp", wk.address)
	if e != nil {
		log.Fatal("worker listen error:", e)
	}
	wk.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-wk.shutdown:
				return
			default:
			}
			conn, err := wk.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !wk.dead {
					log.Fatal("worker accept error: ", err)
				}
			}
		}
	}()

	err := Call(master, "Master.RPCRegisterWorker", RegisterWorkerArg{address}, &RegisterWorkerReply{})
	if err != nil {
		log.Fatal("worker register error: ", err)
	}

	return wk
}

func (wk *Worker) Shutdown() {
	if !wk.dead {
		log.Warning(wk.address, " Shutdown")
		wk.dead = true
		close(wk.shutdown)
		wk.l.Close()
	}
}

// do map task or do reduce task
func (wk *Worker) RPCDoTask(args DoTaskArg, reply *DoTaskReply) error {
	var err error
	if args.Phase == MapPhase {
		err = wk.doMap(args.JobName, args.TaskNo, args.OtherPhaseNum)
	} else if args.Phase == ReducePhase {
		err = wk.doReduce(args.JobName, args.TaskNo, args.OtherPhaseNum)
	} else {
		err = fmt.Errorf("invalid phase")
	}

	return err
}

func (wk *Worker) RPCSetConfig(args SetConfigArg, reply *SetConfigReply) error {
    log.Fatal("unsupported")
    return nil
}

func (wk *Worker) loadSample(jobName string) error {
    log.Info(wk.address, " load sample")
    filename := wk.rootDir + sampleName(jobName)

    f, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer f.Close()

    buf := make([]byte, 1 << 12) // large enough
    _, err = f.Read(buf)
    if err != nil {
        return err
    }

    wk.sample = strings.Split(string(buf), "\n")
    wk.sample = wk.sample[:len(wk.sample) - 1]

    //log.Info(wk.sample)
    return nil
}

func lowerBound(sample []string, value string) int {
    l := 0
    h := len(sample)

    for l < h {
        mid := (l + h) / 2

        if value <= sample[mid] {
            h = mid
        } else {
            l = mid + 1
        }
    }

    return l
}

// map numbers to corresponding reduce task
func (wk *Worker) doMap(jobName string, taskNo int, nOther int) error {
    log.Infof("wk %v do map %v", wk.address, taskNo)

    // read sample
    if len(wk.sample) == 0 {
        err := wk.loadSample(jobName)
        if err != nil {
            return err
        }
    }
    if len(wk.sample) != nOther - 1 {
        return fmt.Errorf("sample and reduce number don't match")
    }

    in, err := NewFileBuffer(wk.rootDir + mapName(jobName, taskNo), StringLength + 1, DefaultBufferSize)
    if err != nil {
        return err
    }
    defer in.Destroy()

    // domap
    for {
        buf, err := in.Get()
        if err != nil && err != io.EOF {
            return err
        }

        // collect numbers
        kvlist := make([][]string, nOther)

        keys := strings.Split(string(buf), "\n")
        keys = keys[:len(keys) - 1] // delete last empty element
        for _, v := range keys{
            if v == "" {
                continue
            }
            t := lowerBound(wk.sample, v)
            kvlist[t] = append(kvlist[t], v)
        }

        // append to file
        for k, v := range kvlist {
            if err := func() error {
                out, err := os.OpenFile(wk.rootDir + reduceName(jobName, taskNo, k), os.O_WRONLY | os.O_APPEND | os.O_CREATE, FilePerm)
                if err != nil {
                    return err
                }
                defer out.Close()

                //log.Info("write ", k, " ", strings.Join(v, "\n"))
                for i := range v {
                    out.WriteString(v[i] + "\n")
                }
                if err != nil {
                    return err
                }
                return nil
            }(); err != nil {
                return err
            }
        }

        if err == io.EOF {
            break
        }
    }

	return nil
}

func (wk *Worker) doReduce(jobName string, taskNo int, nOther int) error {
    log.Infof("wk %v do reduce %v", wk.address, taskNo)

    var all []string

    // read from nMap files
    for i := 0; i < nOther; i++ {
        in, err := NewFileBuffer(wk.rootDir + reduceName(jobName, i, taskNo), StringLength + 1, DefaultBufferSize)
        if err != nil {
            return err
        }
        defer in.Destroy()

        for {
            buf, err := in.Get()
            if err != nil && err != io.EOF {
                return err
            }

            strs := strings.Split(string(buf), "\n")
            strs = strs[:len(strs) - 1]
            all = append(all, strs...)

            if err == io.EOF {
                break
            }
        }
    }

    // sort
    sort.Strings(all)

    // output
    out, err := os.OpenFile(wk.rootDir + mergeName(jobName, taskNo), os.O_CREATE | os.O_WRONLY, FilePerm)
    if err != nil {
        return err
    }
    defer out.Close()

    for i := range all {
        out.WriteString(all[i] + "\n")
    }

	return nil
}

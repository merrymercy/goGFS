package terasort

import (

	"fmt"
	log "github.com/Sirupsen/logrus"

	"net"
	"net/rpc"
    "math"
    "math/rand"
    "sort"
    "io"
	"os"
	"sync"
)

type Master struct {
	address string
    rootDir string
	l       net.Listener

	workerChan chan string
	shutdown   chan struct{}
	dead       bool
}

const (
	WorkerChanSize        = 200

	TeraGenNumber         = 201
	TerasortMapTaskNum    = 5
	TerasortReduceTaskNum = 10
    StringLength          = 10

    FilePerm              = 0755
)

func NewMaster(address, rootDir string) *Master {
	m = &Master{
		address:    address,
        rootDir:    rootDir,
		workerChan: make(chan string, WorkerChanSize),
		shutdown:   make(chan struct{}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !m.dead {
					log.Fatal("master accept error:", err)
				}
			}
		}
	}()

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		log.Warning(m.address, " Shutdown")
		m.dead = true
		close(m.shutdown)
		m.l.Close()
	}
}

func (m *Master) RPCRegisterWorker(args RegisterWorkerArg, reply *RegisterWorkerReply) error {
	log.Info("new worker ", args.Address)
	m.workerChan <- args.Address
	return nil
}

// set job config and do the task
func (m *Master) doJob(jobName string, inFile, outFile string, nMap int, nReduce int) error {
	var err error

	err = m.split(jobName, inFile, nMap)
	if err != nil {
		return err
	}

    err = m.sample(jobName, inFile, nReduce - 1)
    if err != nil {
        return err
    }

	err = m.schedule(jobName, MapPhase, nMap, nReduce)
	if err != nil {
		return err
	}
	err = m.schedule(jobName, ReducePhase, nReduce, nMap)
	if err != nil {
		return err
	}

	err = m.merge(jobName, outFile, nReduce)
	if err != nil {
		return err
	}

	return nil
}

func (m *Master) split(jobName string, inFile string, nMap int) error {
    log.Info("master split...")
    singleSize := int(math.Ceil(float64(TeraGenNumber) / float64(nMap)))

    in, err := os.Open(m.rootDir + inFile)
    if err != nil {
        return err
    }
    defer in.Close()

    buf := make([]byte, (StringLength + 1) * singleSize)

    var pos int64 = 0

    for i := 0; i < nMap; i++ {
        n, err := in.ReadAt(buf, pos)
        if err != nil && err != io.EOF {
            return err
        }

        out, err := os.OpenFile(m.rootDir + mapName(jobName, i), os.O_CREATE | os.O_WRONLY, FilePerm)
        if err != nil {
            return err
        }
        out.Write(buf[:n])
        out.Close()

        pos += int64(n)
    }

	return nil
}

func (m *Master) merge(jobName string, outFile string, nReduce int) error {
    log.Info("master merge...")

    out, err := os.OpenFile(m.rootDir + outFile, os.O_CREATE | os.O_WRONLY, FilePerm)
    if err != nil {
        return err
    }
    defer out.Close()

	for i := 0; i < nReduce; i++ {
        if err := func() error {
            in, err := newFileBuffer(m.rootDir + mergeName(jobName, i), StringLength + 1, DefaultBufferSize)
            if err != nil {
                return err
            }
            defer in.destroy()

            for {
                buf, err := in.get()
                log.Warning(string(buf))
                if err != nil && err != io.EOF {
                    return err
                }

                // append to final file
                _, err2 := out.Write(buf)
                if err2 != nil {
                    return err2
                }

                if err == io.EOF {
                    break
                }
            }
            return nil
        }(); err != nil {
            return err
        }
    }

	return nil
}

func (m *Master) sample(jobName string, inFile string, num int) error {
    in, err := os.Open(m.rootDir + inFile)
    if err != nil {
        return err
    }
    defer in.Close()

    step := TeraGenNumber / num
    tosort := make([]string, num)

    buf := make([]byte, StringLength)
    for i := 0; i < num; i++ {
        pos := (StringLength + 1) * step * i + (rand.Int() % step) * (StringLength + 1)
        _, err := in.ReadAt(buf, int64(pos))

        if err != nil {
            return err
        }
        tosort[i] = string(buf)
    }

    sort.Strings(tosort)

    out, err := os.OpenFile(m.rootDir + sampleName(jobName), os.O_CREATE | os.O_WRONLY, FilePerm)
    if err != nil {
        return err
    }
    defer out.Close()

    for i := range tosort {
        out.WriteString(tosort[i] + "\n")
    }

    return nil
}

func (m *Master) schedule(jobName string, phase JobPhase, nTasks int, nOther int) error {
	var wg sync.WaitGroup

    log.Infof("start phase %v", phase)

	wg.Add(nTasks)
	for i := 0; i < nTasks; i++ {
		go func(x int) {
			args := DoTaskArg{
				JobName:       jobName,
				Phase:         phase,
				TaskNo:        x,
				OtherPhaseNum: nOther,
			}

			for { // infinite try
				worker := <-m.workerChan
                log.Info("use new worker ", worker)
				err := Call(worker, "Worker.RPCDoTask", args, &DoTaskReply{})
				if err == nil {
					m.workerChan <- worker
					break
				} else {
                    log.Errorf("worker %v error %v", worker, err)
                }
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	return nil
}

func (m *Master) TeraHeader(num int, checksum int) string {
    return fmt.Sprintf("Tera %d %d", num, checksum)
}

func (m *Master) TeraSort(in, out string) error {
    m.doJob("Terasort", in, out, TerasortMapTaskNum, TerasortReduceTaskNum)
    return nil
}

func (m *Master) TeraGen(filename string) error {
	f, err := os.OpenFile(m.rootDir + filename, os.O_WRONLY | os.O_CREATE, FilePerm)
    if err != nil {
        return err
    }
    defer f.Close()

    str := make([]byte, StringLength)
	for i := 0; i < TeraGenNumber; i++ {
        for j := 0; j < StringLength; j++ {
            str[j] = byte(rand.Int31() % 26 + 'a')
        }
		f.WriteString(fmt.Sprintf("%v\n", string(str)))
	}

    return nil
}

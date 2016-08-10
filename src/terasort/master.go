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
    "strings"
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

// split inFile into nMap files
func (m *Master) split(jobName string, inFile string, nMap int) error {
    log.Info("master split...")
    singleSize := int(math.Ceil(float64(TeraGenNumber) / float64(nMap)))

    in, err := NewFileBuffer(m.rootDir + inFile, StringLength + 1, (StringLength + 1) * singleSize)
    if err != nil {
        return err
    }
    defer in.Destroy()

    for i := 0; i < nMap; i++ {
        buf, err := in.Get()
        if err != nil && err != io.EOF {
            return err
        }

        out, err := os.OpenFile(m.rootDir + mapName(jobName, i), os.O_CREATE | os.O_WRONLY, FilePerm)
        if err != nil {
            return err
        }
        out.Write(buf)
        out.Close()
    }

	return nil
}

// merge nReduce files into outFile
func (m *Master) merge(jobName string, outFile string, nReduce int) error {
    log.Info("master merge...")

    out, err := os.OpenFile(m.rootDir + outFile, os.O_CREATE | os.O_WRONLY, FilePerm)
    if err != nil {
        return err
    }
    defer out.Close()

	for i := 0; i < nReduce; i++ {
        if err := func() error {
            in, err := NewFileBuffer(m.rootDir + mergeName(jobName, i), StringLength + 1, DefaultBufferSize)
            if err != nil {
                return err
            }
            defer in.Destroy()

            for {
                buf, err := in.Get()
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

// random pick some sample
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

// schedule workers to do the tasks
func (m *Master) schedule(jobName string, phase JobPhase, nTasks int, nOther int) error {
    log.Infof("start phase %v", phase)

	var wg sync.WaitGroup
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

// do the sort
func (m *Master) TeraSort(in, out string) error {
    m.doJob("Terasort", in, out, TerasortMapTaskNum, TerasortReduceTaskNum)
    return nil
}

// generate input file, one string per line
func (m *Master) TeraGen(filename string) error {
	f, err := os.OpenFile(m.rootDir + filename, os.O_WRONLY | os.O_CREATE, FilePerm)
    if err != nil {
        return err
    }
    defer f.Close()

    ans := make([]string, TeraGenNumber)

    log.Info("start generating strings")
    str := make([]byte, StringLength)
	for i := 0; i < TeraGenNumber; i++ {
        for j := 0; j < StringLength; j++ {
            str[j] = byte(rand.Int31() % 26 + 'a')
        }
		f.WriteString(fmt.Sprintf("%v\n", string(str)))

        ans[i] = string(str)
	}
    log.Info("sort answer")

    sort.Strings(ans)

    ansf, err := os.OpenFile(m.rootDir + "ans.txt", os.O_CREATE | os.O_WRONLY, FilePerm)
    if err != nil {
        return err
    }
    _, err = ansf.WriteString(strings.Join(ans, "\n") + "\n")
    if err !=  nil {
        return err
    }
    ansf.Close()

    return nil
}

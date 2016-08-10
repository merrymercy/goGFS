package terasort

import (
    "os"
    "fmt"
    //"strconv"
    "testing"
)

var (
    m     *Master
    wk    []*Worker
    wkAdd []string
    root  string
)

const (
    mAdd = ":6666"
    wkNum = 5
)

const (
    jobName = "terasort"
    inputFile = "raw.txt"
    outputFile = "sorted.txt"
    sampleFile = "sample.txt"
)

func TestGen(t *testing.T) {
    err := m.TeraGen(inputFile)
    if err != nil {
        t.Error(err)
    }
}

func TestSplit(t *testing.T) {
    err := m.split(jobName, inputFile, TerasortMapTaskNum)
    if err != nil {
        t.Error(err)
    }
}

func TestSample(t *testing.T) {
    err := m.sample(jobName, inputFile, TerasortReduceTaskNum - 1)
    if err != nil {
        t.Error(err)
    }
}

func TestMap(t *testing.T) {
    err := m.schedule(jobName, MapPhase, TerasortMapTaskNum, TerasortReduceTaskNum)
	if err != nil {
		t.Error(err)
	}
}

func TestReduce(t *testing.T) {
    err := m.schedule(jobName, ReducePhase, TerasortReduceTaskNum, TerasortMapTaskNum)
	if err != nil {
		t.Error(err)
	}
}

func TestMerge(t *testing.T) {
	err := m.merge(jobName, outputFile, TerasortReduceTaskNum)
	if err != nil {
		t.Error(err)
	}
}

func TestMain(tm *testing.M) {
    root = "./test/"

    os.RemoveAll(root)
    os.Mkdir(root, FilePerm)

    // run master and worker
    m = NewMaster(mAdd, root)
    wkAdd = make([]string, wkNum)
    wk = make([]*Worker, wkNum)
    for i := 0; i < wkNum; i++ {
        wkAdd[i] = fmt.Sprintf(":%v", i + 10000)
        wk[i] = NewWorker(wkAdd[i],mAdd, root)
    }

    ret := tm.Run()

    // shutdown
    for _, v := range wk {
        v.Shutdown()
    }
    m.Shutdown()

    os.Exit(ret)
}

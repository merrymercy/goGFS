package terasort

import (
	"strconv"
)

type JobPhase int

const (
	MapPhase    JobPhase = 1
	ReducePhase          = 2

	TmpFilePrefix        = "mr-"
    FilePerm             = 0755
)

// system config
const (
	TeraGenNumber         = 8 << 20
	TerasortMapTaskNum    = 10
	TerasortReduceTaskNum = 10
    StringLength          = 8 - 1
)

// get the names of input file of task
func mapName(jobName string, mapTask int) string {
	return TmpFilePrefix + jobName + "-map-" + strconv.Itoa(mapTask)
}

func sampleName(jobName string) string {
	return TmpFilePrefix + jobName + "-sample"
}

func reduceName(jobName string, mapTask int, reduceTask int) string {
	return TmpFilePrefix + jobName + "-reduce-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func mergeName(jobName string, reduceTask int) string {
	return TmpFilePrefix + jobName + "-res-" + strconv.Itoa(reduceTask)
}

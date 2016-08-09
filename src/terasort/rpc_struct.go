package terasort

type RegisterWorkerArg struct {
    Address string
}
type RegisterWorkerReply struct {
}

type DoTaskArg struct {
    JobName  string
    Phase    JobPhase
    TaskNo   int
    OtherPhaseNum int
}
type DoTaskReply struct {
}

type SetConfigArg struct {
    Key string
    Value string
}
type SetConfigReply struct {
}

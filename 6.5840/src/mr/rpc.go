package mr

//任务类型
type TaskType int

const (
	TaskTypeMap TaskType = iota //map任务
	TaskTypeReduce //reduce任务
	TaskTypeWait //等待任务
	TaskTypeExit //退出任务
)


// TaskRequestArgs：Worker 发起需要一个任务的RPC请求参数。
// Worker 此时不知道会拿到什么任务，所以参数为空即可。
type TaskRequestArgs struct{}

// TaskRequestReply：Coordinator 返回当前分配的任务。
// 由 Coordinator 根据当前状态填充，Worker 根据 Type 和字段执行对应任务。
type TaskRequestReply struct {
	Type TaskType // 分配到的任务类型：Map / Reduce / Wait / Exit

	// Map 任务时有效
	FileName string // 文件名
	MapID    int // map任务ID

	// Reduce 任务时有效
	ReduceID int // reduce任务ID

	// 两种任务都可能用到
	NMap    int // 总 map 数，Reduce 时用来读 mr-0-Y, mr-1-Y, ...
	NReduce int // 总 reduce 数，Map 时用来写 mr-X-0, mr-X-1, ...
}


// TaskResponseArgs：Worker 上报某个任务已完成。
type TaskResponseArgs struct {
	Type   TaskType // 完成的是 Map 还是 Reduce
	TaskID int      // Map 时为 MapID，Reduce 时为 ReduceID
}

// TaskResponseReply：上报的返回值，无额外信息，但 RPC 必须要有回复。
type TaskResponseReply struct{}
//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskStatus int //任务状态
//type CurrentTaskStatus int //当前任务状态
type CurrentTaskPhase int //当前任务阶段

//任务状态
const (
	TaskStatusIdle TaskStatus = iota // 空闲状态
	TaskStatusInProcess // 正在处理状态
	TaskStatusCompleted // 已完成状态
)
// //当前任务状态
// const (
// 	CurrentTaskStatusIdle CurrentTaskStatus = iota // 空闲状态
// 	CurrentTaskStatusInProcess // 正在处理状态
// 	CurrentTaskStatusCompleted // 已完成状态
// )
//当前任务阶段
const (
	CurrentTaskPhaseMap CurrentTaskPhase = iota // map阶段
	CurrentTaskPhaseReduce // reduce阶段
	CurrentTaskPhaseDone // 完成阶段
)


type Coordinator struct {
	// Your definitions here.
	Mutex sync.Mutex // 互斥锁
	Files []string // 输入文件列表
	NMap int // map任务数量
	NReduce int // reduce任务数量
	MapTaskStatus []TaskStatus // map任务状态
	ReduceTaskStatus []TaskStatus // reduce任务状态

    //任务开始时间
	MapTaskStartTime []time.Time // map任务开始时间
	ReduceTaskStartTime []time.Time // reduce任务开始时间


	//CurrentTaskStatus CurrentTaskStatus // 当前任务状态
	CurrentTaskPhase CurrentTaskPhase // 当前任务阶段
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//对外的任务请求接口
func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error{
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if(c.CurrentTaskPhase == CurrentTaskPhaseDone){
		reply.Type = TaskTypeExit;//设置为退出状态
		return nil;
	}
	if(c.CurrentTaskPhase == CurrentTaskPhaseMap){//map阶段
		var count int = 0;
		var flag bool = false;
		//找一个空闲的map任务
		for i:=0;i<c.NMap;i++{
			if(c.MapTaskStatus[i] == TaskStatusIdle){
				flag = true;
				c.MapTaskStatus[i] = TaskStatusInProcess;//设置为正在处理状态
				//设置状态后记录时间
				c.MapTaskStartTime[i]=time.Now()
				reply.Type = TaskTypeMap
				reply.MapID = i
				reply.FileName = c.Files[i]
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				return nil
			}else if(c.MapTaskStatus[i] == TaskStatusCompleted){
				count++;
			}
		}
		// 先判断：是否全部 map 都已完成，是则进入 reduce 阶段（不要先 return Wait）
		if count == c.NMap {
			c.CurrentTaskPhase = CurrentTaskPhaseReduce
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			// 不 return，继续往下执行 Reduce 分配
		} else if !flag {
			// 没有空闲的 map 且尚未全部完成，才返回等待
			reply.Type = TaskTypeWait
			return nil
		}
	}

	//reduce阶段-如果上面没有return，则说明是map阶段结束，进入reduce阶段
	if(c.CurrentTaskPhase == CurrentTaskPhaseReduce){//reduce阶段
		var count int = 0;
		for i:=0;i<c.NReduce;i++{
			if(c.ReduceTaskStatus[i] == TaskStatusIdle){
				c.ReduceTaskStatus[i] = TaskStatusInProcess;//设置为正在处理状态
				//设置状态后记录时间
				c.ReduceTaskStartTime[i]=time.Now()
				reply.Type = TaskTypeReduce
				reply.ReduceID = i
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				return nil
			}else if(c.ReduceTaskStatus[i] == TaskStatusCompleted){
				count++;
			}
		}

		if(count != c.NReduce){//如果还有未完成的reduce任务，则返回等待状态,让worker等待
			reply.Type = TaskTypeWait
		}else{
			//所有reduce任务都已完成，则进入完成阶段
			c.CurrentTaskPhase = CurrentTaskPhaseDone;//设置为完成阶段
			//c.CurrentTaskStatus = CurrentTaskStatusIdle;//设置为空闲状态
			reply.Type = TaskTypeExit;//设置为退出状态
			//不需要return
		}
	}
	return nil
}

//worker报告任务完成接口
func (c *Coordinator) ReportTask(args *TaskResponseArgs, reply *TaskResponseReply) error{
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	//根据任务类型设置任务状态
	if(args.Type == TaskTypeMap){
		if(args.TaskID < 0 || args.TaskID >= c.NMap){
			return nil
		}
		c.MapTaskStatus[args.TaskID] = TaskStatusCompleted;//设置为已完成状态
	}else if(args.Type == TaskTypeReduce){
		if(args.TaskID < 0 || args.TaskID >= c.NReduce){
			return nil
		}
		c.ReduceTaskStatus[args.TaskID] = TaskStatusCompleted;//设置为已完成状态
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	ret := false
	// Your code here.
	if(c.CurrentTaskPhase == CurrentTaskPhaseDone){
		ret = true;
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	// Your code here.
	flen:=len(files)
    coordinator:=Coordinator{
	Files: files,
	NMap: flen,
	NReduce: nReduce,
	MapTaskStatus: make([]TaskStatus, flen),
	ReduceTaskStatus: make([]TaskStatus, nReduce),
	MapTaskStartTime:make([]time.Time,flen),
	ReduceTaskStartTime:make([]time.Time,nReduce),
	//CurrentTaskStatus: CurrentTaskStatusIdle,
	CurrentTaskPhase: CurrentTaskPhaseMap,
	}

	//启动gorouting管理超时任务
	go coordinator.watchTasks()
	coordinator.server(sockname)
	// //启动gorouting管理超时任务
	// go coordinator.watchTasks()
	return &coordinator
}

func (c *Coordinator) watchTasks() {
    for {
        time.Sleep(500 * time.Millisecond) // 每半秒检查一次
        c.Mutex.Lock()
        
        // 如果全部完成，退出协程
        if c.CurrentTaskPhase == CurrentTaskPhaseDone {
            c.Mutex.Unlock()
            return
        }

        // 检查 Map 超时
        if c.CurrentTaskPhase == CurrentTaskPhaseMap {
            for i := 0; i < c.NMap; i++ {
                if c.MapTaskStatus[i] == TaskStatusInProcess && time.Since(c.MapTaskStartTime[i]) > 30*time.Second {
                    log.Printf("检测到 Map 任务 %d 超时，重置为 Idle", i)
                    c.MapTaskStatus[i] = TaskStatusIdle
                }
            }
        }

        // 检查 Reduce 超时
        if c.CurrentTaskPhase == CurrentTaskPhaseReduce {
            for i := 0; i < c.NReduce; i++ {
                if c.ReduceTaskStatus[i] == TaskStatusInProcess && time.Since(c.ReduceTaskStartTime[i]) > 30*time.Second {
                    log.Printf("检测到 Reduce 任务 %d 超时，重置为 Idle", i)
                    c.ReduceTaskStatus[i] = TaskStatusIdle
                }
            }
        }
        c.Mutex.Unlock()
    }
}
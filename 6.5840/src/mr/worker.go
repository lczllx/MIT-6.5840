package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io"
import "encoding/json"
import "sort"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var lastDialLog time.Time // 上次打印 dial 失败的时间，用于限流避免刷屏
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for{	
		args:=TaskRequestArgs{}
		reply:=TaskRequestReply{}
		ret:=call("Coordinator.RequestTask", &args, &reply)
		//如果调用失败，则等待1秒后继续
		if(!ret){
			time.Sleep(1 * time.Second)
			continue;
		}
		//如果返回类型为退出，则退出循环
		if(reply.Type == TaskTypeExit){
			break;
		}
		//如果返回类型为map，则执行map任务；仅成功时才上报，失败则不报让 coordinator 可重试
		if reply.Type == TaskTypeMap {
			if err := onMapTask(&reply, mapf); err != nil {
				log.Printf("map task %d failed: %v", reply.MapID, err)
				continue
			}
			ret := call("Coordinator.ReportTask",
				&TaskResponseArgs{Type: TaskTypeMap, TaskID: reply.MapID},
				&TaskResponseReply{})
			if !ret {
				continue
			}
			continue
		}
		//如果返回类型为reduce，则执行reduce任务；仅成功时才上报
		if reply.Type == TaskTypeReduce {
			if err := onReduceTask(&reply, reducef); err != nil {
				log.Printf("reduce task %d failed: %v", reply.ReduceID, err)
				continue
			}
			ret:=call("Coordinator.ReportTask", 
			&TaskResponseArgs{Type: TaskTypeReduce, TaskID: reply.ReduceID},
			 &TaskResponseReply{})
			//如果调用失败
			if(!ret){
			continue;
			}
			continue;
		}
		//如果返回类型为等待
		if reply.Type == TaskTypeWait {
			time.Sleep(200 * time.Millisecond)
			continue
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		// coordinator 未启动时每 10 秒最多打一次，避免刷屏
		if time.Since(lastDialLog) >= 10*time.Second {
			log.Printf("dialing coordinator: %v (will retry)", err)
			lastDialLog = time.Now()
		}
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func onMapTask(task_req_reply *TaskRequestReply, mapf func(string, string) []KeyValue)error {
	//使用os.Open()打开文件读取
	file, err := os.Open(task_req_reply.FileName)
	if err != nil {
		//返回错误信息
		return fmt.Errorf("open file failed: %v", err)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		//返回错误信息
		return fmt.Errorf("read file failed: %v", err)
	}
	kv_all:=mapf(task_req_reply.FileName, string(content))//调用mapf函数处理文件内容
	//根据reduse数量分桶
	map_table:=make([][]KeyValue,task_req_reply.NReduce)
	for _,kv:=range kv_all{
		index:=ihash(kv.Key) % task_req_reply.NReduce
		map_table[index] = append(map_table[index], kv)
	}
	// 对每个 reduce 写一个中间文件：mr-MapID-ReduceID
	for r, kvs := range map_table {
		midfilename := fmt.Sprintf("mr-%d-%d", task_req_reply.MapID, r)
		tmpname := fmt.Sprintf("%s-%d", midfilename, os.Getpid())
		//创建临时文件
		ofile, err := os.Create(tmpname)
		if err != nil {
			return fmt.Errorf("cannot create %v: %v", tmpname, err)
		}
		//创建json编码器
		enc := json.NewEncoder(ofile)
		//编码kv 把kv从桶里面逐行写入临时文件
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				//关闭文件
				ofile.Close()
				//返回错误信息
				return fmt.Errorf("encode kv failed: %v", err)
			}
		}
		//关闭文件
		if err := ofile.Close(); err != nil {
			return fmt.Errorf("close file %v failed: %v", tmpname, err)
		}

		// 原子替换 将临时文件重命名为中间文件-使用os.Rename()函数实现原子操作
		if err := os.Rename(tmpname, midfilename); err != nil {
			return fmt.Errorf("rename %v to %v failed: %v", tmpname, midfilename, err)
		}
	}

	return nil
}
func onReduceTask(reply *TaskRequestReply, reducef func(string, []string) string) error {
	//定义中间文件数组
	intermediate := []KeyValue{}

	// 从所有 Map 任务的输出中汇总本 Reduce 需要的键值
	for m := 0; m < reply.NMap; m++ {
		iname := fmt.Sprintf("mr-%d-%d", m, reply.ReduceID)
		file, err := os.Open(iname)
		if err != nil {
			// 某些 map 可能失败/未生成，对 crash 容错时 coordinator 会重试，这里忽略不存在的文件
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				return fmt.Errorf("decode intermediate %v failed: %v", iname, err)
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	// 按 key 排序
	sort.Sort(ByKey(intermediate))

	// 输出结果到 mr-out-ReduceID
	oname := fmt.Sprintf("mr-out-%d", reply.ReduceID)
	tmpname := fmt.Sprintf("%s-%d", oname, os.Getpid())

	ofile, err := os.Create(tmpname)
	if err != nil {
		return fmt.Errorf("cannot create %v: %v", tmpname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		sort.Strings(values) // 保证与 mrsequential 一致，indexer 等对 value 顺序敏感

		output := reducef(intermediate[i].Key, values)
		// 这一行输出格式必须与顺序版一致
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	if err := ofile.Close(); err != nil {
		return fmt.Errorf("close %v failed: %v", tmpname, err)
	}

	if err := os.Rename(tmpname, oname); err != nil {
		return fmt.Errorf("rename %v to %v failed: %v", tmpname, oname, err)
	}

	return nil
}
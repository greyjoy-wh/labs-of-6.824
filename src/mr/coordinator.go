package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	FileName string //文件名
	send     bool   //是否被发布
	Finish   bool   //是否有被完成
}

type ReduceTask struct {
	send   bool //是否被发布
	Finish bool //是否有被完成
}

//MapTask map[int]

type Coordinator struct {
	// Your definitions here.
	MapTasks map[int]*MapTask
	// MapTask map[string]bool //待处理的map任务 map[文件名字]是否完成
	ReduceTasks map[int]*ReduceTask
	MapMux      sync.Mutex
	// ReduceTask map[string]bool //待处理的reduce任务
	ReduceMux sync.Mutex
	nReduce   int //设置有几个reduce任务，也就是map worker要输出的文件的哈希取模值
	mapNum    int //当前还剩几个mapNum没有被finished
	reduceNum int //当前还剩几个reduceNum没有有finished
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) startCounting(Tasktype int, key int) {
	if Tasktype == 0 {
		//如果是map任务计时
		time.Sleep(time.Second * 10) //睡眠10秒等待 任务完成
		c.MapMux.Lock()
		defer c.MapMux.Unlock()
		if c.MapTasks[key].Finish {
			//如果完成了 结束该任务
			return
		} else {
			//没有完成 直接将该任务设置为没有发送状态
			c.MapTasks[key].send = false //重新去
			return
		}
	} else {
		//如果是reduce任务计时
		time.Sleep(time.Second * 10) //睡眠10秒等待 任务完成
		c.ReduceMux.Lock()
		defer c.ReduceMux.Unlock()
		if c.ReduceTasks[key].Finish {
			//如果完成了 结束该任务
			return
		} else {
			//没有完成 直接将该任务设置为没有发送状态
			c.ReduceTasks[key].send = false //重新去
			return
		}
	}
}

func (c *Coordinator) TaskAsk(args *TaskAskRequest, reply *TaskAskResponse) error {
	//遍历所有的map任务，找到没有被执行的，也就是value为false的
	//判断任何coordinator中的量的时候是不是要用锁？

	//遍历的过程要锁吧
	c.MapMux.Lock()
	defer c.MapMux.Unlock()
	for key, value := range c.MapTasks {
		if value.send { //如果是 true  那么就是被分发了 就不管了
			continue
		} else {
			reply.Filename = value.FileName
			reply.TaskType = 0
			reply.nReduce = c.nReduce
			reply.mapNum = key          //第几个map任务
			c.MapTasks[key].send = true //表示当前任务已经发派
			//分配任务后要开始计时等待
			go c.startCounting(0, key)
			// c.mapNum += 1
			return nil
		}
	}
	if c.mapNum > 0 {
		//如果所有任务都是send  但是没有全部是finished的 那么就发送wait的信息
		reply.wait = true
		return nil
	}

	//如果所有的map任务全部完成了 就可以分发reduce任务
	for key, value := range c.ReduceTasks {
		if value.send {
			continue
		} else {
			reply.TaskType = 1
			reply.reduceNum = key
			c.ReduceTasks[key].send = true
			go c.startCounting(1, key)
			return nil
		}
	}

	if c.reduceNum > 0 {
		//如果所有任务都是send  但是没有全部是finished的 那么就发送wait的信息
		reply.wait = true
		return nil
	}
	// for c.reduceNum < c.nReduce {
	// 	reply.TaskType = 1
	// 	reply.nReduce = c.nReduce
	// 	reply.reduceNum = c.reduceNum
	// 	c.reduceNum += 1
	// 	return nil
	// }
	//没任务了就直接返回没任务
	reply.AllDone = true
	return nil
}

func (c *Coordinator) OneMapFinish(args *MapFinishRequest, reply *MapFinishResponse) error {
	c.MapMux.Lock()
	defer c.MapMux.Unlock()
	c.MapTasks[args.mapNum].Finish = true
	c.mapNum = c.mapNum - 1
	return nil
}

func (c *Coordinator) OneReduceFinish(args *MapFinishRequest, reply *MapFinishResponse) error {
	c.ReduceMux.Lock()
	defer c.ReduceMux.Unlock()
	c.ReduceTasks[args.mapNum].Finish = true
	c.reduceNum = c.reduceNum - 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  //将coordinator 注册到rpc上
	rpc.HandleHTTP() //通过http进行rpc通信
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() //本地通信的地址，是一个文件
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.reduceNum == 0 {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks: make(map[int]*MapTask),
		// ReduceTask: make(map[string]bool),
		ReduceTasks: make(map[int]*ReduceTask),
		nReduce:     nReduce,
		reduceNum:   nReduce,
	}
	//创建 nreduce个任务
	for i := nReduce; i < nReduce; i++ {
		c.ReduceTasks[i].send = false
		c.ReduceTasks[i].Finish = false
	}

	//遍历文件 添加进map任务中
	n := 0
	for _, str := range files {
		thisMapTask := new(MapTask)
		// thisMapTask := MapTask{} //这是在栈上创建
		thisMapTask.FileName = str
		thisMapTask.Finish = false
		thisMapTask.send = false
		c.MapTasks[n] = thisMapTask
		n = n + 1
	}
	c.mapNum = n //一共有几个map任务，即有几个文件
	// Your code here.

	c.server()
	return &c
}

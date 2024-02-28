package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } //比较key的大小

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

// main/mrworker.go calls this function.
//
// 通过RPC判断要分配的任务是什么
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	//获得需要操作的文件 以及任务的类型 Map or reduce

	for { //循环获取任务
		//远程调用TaskAsk，获取任务
		//每次rpc之前都要创建全新的这两个
		args := TaskAskRequest{}
		reply := TaskAskResponse{}
		args.WorkID = 1 //暂定为1
		// println("正在寻求任务")
		call("Coordinator.TaskAsk", &args, &reply)
		if reply.AllDone {
			//所有任务都完成了 可以退休了 *****
			return
		}

		//如果是wait 那么 就等待1s再去询问任务
		if reply.Wait {
			time.Sleep(time.Second * 1)
			continue
		}

		taskType := reply.TaskType

		if taskType == 0 {
			//执行map任务
			filename := reply.Filename
			nReduce := reply.NReduce

			mapNum := reply.MapNum
			// println("已经接收到map任务 ", reply.MapNum)
			// println("已经接收到任务map名字 ", filename)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva)) //直接就对kva 排序，而不是对文件排序
			// intermediate = append(intermediate, kva...)
			// sort.Sort(ByKey(intermediate)) //按照key进行排序
			//得到所有的的kv对 都在 kva数组里面，现在要将他们分别写入不同文件中

			//创建10个临时文件
			fileMap := make(map[int]*os.File)
			for i := 0; i < nReduce; i++ {
				file, _ := ioutil.TempFile("", "Temp")
				fileMap[i] = file
			}

			for _, kv := range kva {
				reduceNum := ihash(kv.Key) % nReduce
				// interFileName := fmt.Sprintf("mr-%v-%v", mapNum, reduceNum)
				// interFile, err := os.OpenFile(interFileName, os.O_WRONLY|os.O_CREATE, 0666)
				// interFile, err := os.Create(interFileName)
				// if err != nil {
				// 	log.Fatalf("cannot open %v", interFileName)
				// }
				enc := json.NewEncoder(fileMap[reduceNum])
				err1 := enc.Encode(&kv)
				if err1 != nil {
					log.Fatal(err1)
				}
			}
			//当写入的临时文件全部写完后 将10个文件夹全部转化为该有的名字，原子命名
			for num, file := range fileMap {
				interFileName := fmt.Sprintf("mr-%v-%v", mapNum, num)
				os.Rename(file.Name(), interFileName)
			}

			//rpc告诉coordinator map任务完成
			req := MapFinishRequest{}
			// req.mapFileName = filename
			req.MapNum = mapNum
			reply := MapFinishResponse{}
			call("Coordinator.OneMapFinish", &req, &reply)
			// println("wor中 map任务", req.MapNum, "已经完成")
			// oname := fmt.Sprint("mr-%v-0", filename) //map输出的中间文件
			// ofile, _ := os.Create(oname)

		} else if taskType == 1 {
			//执行reduce任务
			var wg sync.WaitGroup
			var mu sync.Mutex
			kva := []KeyValue{} //所有的key和vaule值
			reduceNum := reply.ReduceNum
			// println("已经接收到reduce任务 ", reply.ReduceNum)
			//这时目标要写入的数据
			oname := fmt.Sprintf("mr-out-%v", reduceNum)
			ofile, _ := os.Create(oname)

			//打开所有的名字中包含 mr-*-reduceNum的文件
			filenames, err := filepath.Glob(fmt.Sprintf("mr-*-%v", reduceNum))
			if err != nil {
				fmt.Printf("Error matching pattern: %v\n", err)
				return
			}
			/** 串行处理  不能通过reduce parallelism test
				for _, filename := range filenames {
				// 打开文件
				file, err := os.Open(filename)
				if err != nil {
					fmt.Printf("Error opening file %s: %v\n", filename, err)
					continue
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				// 处理打开的文件
				// ...
			}**/
			//多协程进行解码
			for _, filename := range filenames {
				wg.Add(1)
				go func(filename string) {
					defer wg.Done()
					file, err := os.Open(filename)
					if err != nil {
						fmt.Printf("Error opening file %s: %v\n", filename, err)
						return
					}
					defer file.Close()
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						mu.Lock()
						kva = append(kva, kv)
						mu.Unlock()
					}
				}(filename)
			}
			wg.Wait()

			sort.Sort(ByKey(kva)) //按照key再次进行排序
			intermediate := kva
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
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			//告诉coordinator 我reduce完成了
			req := ReduceFinishRequest{}
			// req.mapFileName = filename
			req.ReducepNum = reduceNum
			reply := ReduceFinishResponse{}
			call("Coordinator.OneReduceFinish", &req, &reply)
		} else {
			fmt.Println("task 类型没见过")
		}

	}
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

	// send the RPC request, wait for the reply.  调用coordinator的方法
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

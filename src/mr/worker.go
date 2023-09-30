package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"
import "time"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	flag := false
	for {
		task := Task{}
		args := ApplyTaskArgs{}
		if flag {
			task.TaskType = Waiting
			args.Status = Waiting // 告知master我在waiting
			flag = false
		}
		ok := call("Coordinator.ApplyForTask", &args, &task)
		if ok {
			switch task.TaskType {
			case Waiting: // 通知worker该waiting
				{
					time.Sleep(time.Second)
					flag = true
				}
			case Map:
				{
					DoMapTask(mapf, &task)
					FinishTask(&task)
				}
			case Reduce:
				{
					DoReduceTask(reducef, &task)
					FinishTask(&task)
				}
			default:
				{
					goto END
				}

			}
		} else {
			break
		}
	}
END:
	log.Printf("工作完成")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func FinishTask(task *Task) {
	reply := ApplyTaskReply{}
	call("Coordinator.FinishTask", task, &reply)
}

func GetTask(reply *Task) bool {
	args := ApplyTaskArgs{}
	if reply.TaskType == Waiting {
		args.Status = Waiting
	}
	ok := call("Coordinator.ApplyForTask", &args, reply)
	return ok
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	Filename := task.Filename
	file, err := os.Open(Filename)
	if err != nil {
		log.Fatalf("cannot open %v", Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Filename)
	}
	file.Close()
	kva := mapf(Filename, string(content))

	intermediateMap := make([][]KeyValue, task.Num)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.Num
		intermediateMap[idx] = append(intermediateMap[idx], kv)
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < task.Num {
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediateMap[i] {
			enc.Encode(&kv)
		}
		i++
		ofile.Close()
	}

}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	i := 0
	kva := []KeyValue{}
	for i < task.Num {
		filepath := task.Filename + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId)
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
		i++
	}
	sort.Sort(ByKey(kva))
	i = 0
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	//dir, _ := os.Getwd()
	//tempFile, _ := ioutil.TempFile(dir, "mr-tmp-*")
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
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

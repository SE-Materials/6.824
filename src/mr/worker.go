package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	exitForLoop := false
	for {
		task := GetTask()

		switch task.TaskType {

		case WorkerTaskType_Map:
			performMapTask(mapf, task)

		case WorkerTaskType_Reduce:
			performReduceTask(reducef, task)

		case WorkerTaskType_Exit:
			exitForLoop = true
		}

		if exitForLoop {
			break
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func performMapTask(mapf func(string, string) []KeyValue, task Reply) {
	filename := task.Filename
	log.Println("Seq: ", task.Seq)
	if len(filename) > 0 {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			err := fmt.Errorf("cannot open %v", filename)
			FinishMapTask(filename, WorkerTaskType_Map, err)
			return
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			err := fmt.Errorf("cannot read %v", filename)
			FinishMapTask(filename, WorkerTaskType_Map, err)
			return
		}

		file.Close()
		kva := mapf(filename, string(content))

		reduces := make([][]KeyValue, task.NReduce)
		for _, kv := range kva {
			idx := ihash(kv.Key) % task.NReduce
			reduces[idx] = append(reduces[idx], kv)
		}

		for idx, intermediate := range reduces {
			filename := generateMapResultFileName(idx, task.Seq)
			f, err := os.Create(filename)
			if err != nil {
				FinishMapTask(filename, WorkerTaskType_Map, err)
				return
			}

			enc := json.NewEncoder(f)
			for _, kv := range intermediate {
				err = enc.Encode(&kv)
				if err != nil {
					FinishMapTask(filename, WorkerTaskType_Map, err)
					return
				}
			}

			err = f.Close()
			if err != nil {
				FinishMapTask(filename, WorkerTaskType_Map, err)
				return
			}
		}
		log.Printf("[%d] %v done.\n", os.Getpid(), filename)

		FinishMapTask(filename, WorkerTaskType_Map, nil)

	} else {
		log.Printf("[%d] Didn't get any tasks!\n", os.Getpid())
	}
}

func performReduceTask(reducef func(string, []string) string, task Reply) {
	maps := make(map[string][]string)
	var kva []KeyValue

	for idx := 0; idx < task.NMap; idx++ {
		filename := generateMapResultFileName(idx, task.Seq)
		file, err := os.Open(filename)
		if err != nil {
			FinishReduceTask(task.Seq, err)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				FinishReduceTask(task.Seq, err)
				break
			}

			kva = append(kva, kv)
		}
	}

	for _, kv := range kva {
		maps[kv.Key] = append(maps[kv.Key], kv.Value)
	}

	var buf bytes.Buffer
	for key, values := range maps {
		output := reducef(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}

	err := ioutil.WriteFile(generateReduceResultFileName(task.Seq), buf.Bytes(), 0600)
	if err != nil {
		FinishReduceTask(task.Seq, err)
	}
	FinishReduceTask(task.Seq, nil)
}

func GetTask() Reply {
	args := Arg{
		WorkerId: os.Getpid(),
	}
	reply := Reply{}
	call("Master.RequestTask", &args, &reply)
	log.Printf("[%d] : %v\n", os.Getpid(), reply.Filename)
	return reply
}

func FinishMapTask(filename string, taskType WorkerTaskType, err error) {
	args := FinishResponse{
		WorkerId: os.Getpid(),
		Filename: filename,
		TaskType: taskType,
		Error:    err,
	}
	reply := Reply{}
	call("Master.FinishTask", &args, &reply)
}

func FinishReduceTask(id int, err error) {
	args := FinishResponse{
		WorkerId: os.Getpid(),
		TaskType: WorkerTaskType_Reduce,
		Error:    err,
		Seq:      id,
	}
	reply := Reply{}
	call("Master.FinishTask", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

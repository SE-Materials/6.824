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

const TIMEOUT = 100 * time.Second

// Task status enum
type TaskStatus byte
type Phase byte

const (
	InQueue TaskStatus = iota
	WithWorker
	Finished
	Failed
)

const (
	Map Phase = iota
	Reduce
	Complete
)

type Task struct {
	id        int
	filename  string
	startTime time.Time
	status    TaskStatus
}

type Master struct {
	phase       Phase
	files       []string
	mapTasks    []*Task
	reduceTasks []*Task
	nMap        int
	nReduce     int
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) RequestTask(args *Arg, reply *Reply) error {

	if m.phase == Map {
		mu.Lock()
		defer mu.Unlock()

		// foundTask := false
		for idx, task := range m.mapTasks {
			if task.status == InQueue || task.status == Failed || time.Now().Sub(task.startTime) > TIMEOUT {
				task.status = WithWorker
				task.startTime = time.Now()
				reply.Filename = task.filename
				reply.TaskType = WorkerTaskType_Map
				reply.NReduce = m.nReduce
				reply.NMap = m.nMap
				reply.Seq = idx
				log.Printf("[%v] took up map task for %v\n", args.WorkerId, task.filename)
				// foundTask = true
				break
			}
		}

		// if foundTask == false {
		// reply.TaskType = WorkerTaskType_Exit
		// }
	}

	if m.phase == Reduce {
		mu.Lock()
		defer mu.Unlock()

		for idx, task := range m.reduceTasks {
			if task.status == InQueue || task.status == Failed || time.Now().Sub(task.startTime) > TIMEOUT {
				task.status = WithWorker
				task.startTime = time.Now()
				task.id = idx
				reply.TaskType = WorkerTaskType_Reduce
				reply.NReduce = m.nReduce
				reply.NMap = m.nMap
				reply.Seq = idx
				log.Printf("[%v] took up reduce task for %v\n", args.WorkerId, idx)
				break
			}
		}

	}

	return nil
}

func (m *Master) FinishTask(args *FinishResponse, reply *Reply) error {

	if m.phase == Map {
		mu.Lock()
		defer mu.Unlock()
		taskToRemove := -1
		nTasksDone := 0

		for i, task := range m.mapTasks {
			if task.status == Finished {
				nTasksDone++
			} else if task.filename == args.Filename {

				if args.Error != nil {
					log.Printf("[%v] errored out %v for %v.\n", args.WorkerId, args.Error, task.filename)

				} else {
					task.status = Finished
					taskToRemove = i
					nTasksDone++
					log.Printf("[%v] finished %v.\n", args.WorkerId, task.filename)
				}
				// break
			}
		}

		log.Printf("Out of %v tasks, %v completed\n", len(m.mapTasks), nTasksDone)

		if nTasksDone >= len(m.mapTasks)-1 {
			log.Println("==== Completed all map tasks. Starting Reduce tasks ====")
			m.phase = Reduce
			m.reduceTasks = make([]*Task, m.nReduce)
		}

		if taskToRemove == -1 {
			log.Printf("Found no matching tasks to finish\n")
		} else {
			// n := len(m.mapTasks)
			// m.mapTasks[taskToRemove] = m.mapTasks[n-1]
			// m.mapTasks = m.mapTasks[:n-1]
			// m.mapTasks = append(m.mapTasks[:taskToRemove], m.mapTasks[taskToRemove+1])
		}
	} else if m.phase == Reduce {
		mu.Lock()
		defer mu.Unlock()

		nTasksDone := 0

		for _, task := range m.reduceTasks {
			if task.status == Finished {
				nTasksDone++
			}

			if task.id == args.Seq {

				if args.Error != nil {
					log.Printf("[%v] errored out %v.\n", args.WorkerId, args.Error)

				} else {
					task.status = Finished
					log.Printf("[%v] finished %v.\n", args.WorkerId, task.id)
				}
				break
			}
		}

		if nTasksDone == len(m.reduceTasks) {
			log.Println("==== Completed all reduce tasks. Combining results ====")
			m.phase = Complete
		}
	}

	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	ret := (m.phase == Complete)

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:   files,
		nReduce: nReduce,
		nMap:    len(files),
	}
	for _, filename := range files {
		m.mapTasks = append(m.mapTasks, &Task{
			filename: filename,
			status:   InQueue,
		})
	}
	m.server()
	return &m
}

package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) DoJob(work JobType,sync chan string,jobNumber int) {
	workerAddress :=  <- mr.registerChannel
	var k int
    DPrintf("DoJob: map/reduce")
	switch(work){
	case Map: 
	k = mr.nReduce

	case Reduce: 
	k=mr.nMap
	}	
	workerAddress = workerAddress
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = work
	args.JobNumber = jobNumber    
	args.NumOtherPhase =  k 

	var reply DoJobReply;
	ok := call(workerAddress, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("DoWork: RPC DpJob error")
		mr.DoJob(work,sync,jobNumber)
	} else {
		sync <- "done"
	}
	mr.registerChannel <- workerAddress;

}


func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  var work JobType
  work = "Map"
  sync := make(chan string)
  
  for i:=0;i<mr.nMap;i++ {
	  go mr.DoJob(work,sync,i)
  }

  for i:=0;i<mr.nMap;i++{
	  <-sync
  }
  work = "Reduce"
  mr.jobsCompleted = 0
  for i:=0; i<mr.nReduce ;i++ {
	  go mr.DoJob(work,sync,i)
  }
  for i:=0;i<mr.nReduce;i++{
	  <-sync
  }
  return mr.KillWorkers()
}

package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq    int
	OpType string
	Key    string
	Value  string
	OpId   int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kvstore     map[string]string
	syncedTill  int              //This is the sequence number till which the kv store is up to date
	answerStore map[int64]string //This stores the answers to pass unreliable
}

func (kv *KVPaxos) syncOps(details Op) string {
	// Need to figure a way of modifying to
	to := 10 * time.Millisecond
	kv.mu.Lock()
	val, ok := kv.answerStore[details.OpId]
	if ok {
		fmt.Print("Answer already present")
		fmt.Println(details.OpId)
		kv.mu.Unlock()
		return val
	}
	ans := ""
	for {
		seq := kv.syncedTill // to = 10 * time.Millisecond
		decided, value := kv.px.Status(seq)
		if decided {
			ans = ""
			v := value.(Op)
			fmt.Print("Paxos done for")
			fmt.Println(kv.syncedTill, seq, v.OpId, kv.kvstore[v.Key])
			if v.OpType == "Put" {
				ans = kv.kvstore[v.Key]
				kv.kvstore[v.Key] = v.Value
			} else if v.OpType == "Put hash" {
				ans = kv.kvstore[v.Key]
				kv.kvstore[v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
				// kv.kvstore[v.Key] = hash(ans + v.Value)
			} else if v.OpType == "Get" {
				ans = kv.kvstore[v.Key]
			}
			fmt.Print("Adding to store ")
			kv.answerStore[v.OpId] = ans
			fmt.Println(kv.answerStore[v.OpId])
			if details.OpId == v.OpId {
				break
			}

			// kv.px.Done(seq)
			seq++
			kv.syncedTill++

		} else {
			details.Seq = seq
			fmt.Print("Start Paxos For")
			fmt.Println(details.OpId)
			kv.px.Start(seq, details)
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}
	kv.px.Done(kv.syncedTill)
	kv.syncedTill++
	fmt.Print("Unlocked")
	fmt.Println(kv.me)
	kv.mu.Unlock()
	return ans
}
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	operationDetails := new(Op)
	operationDetails.OpType = "Get"
	operationDetails.Key = args.Key
	operationDetails.Value = ""
	operationDetails.OpId = args.Nrand
	reply.Value = kv.syncOps(*operationDetails)
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	operationDetails := new(Op)
	if args.DoHash == false {
		operationDetails.OpType = "Put"
	} else {
		operationDetails.OpType = "Put hash"
	}
	operationDetails.Key = args.Key
	operationDetails.Value = args.Value
	operationDetails.OpId = args.Nrand
	reply.PreviousValue = kv.syncOps(*operationDetails)
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kvstore = make(map[string]string)
	kv.answerStore = make(map[int64]string)
	kv.syncedTill = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
	//To compare maps
	"reflect"
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
	Seq       int
	OpType    string
	Key       string
	Value     string
	OpId      int64
	NewConfig map[int]bool
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	syncedTill  int //This is the sequence number till which the kv store is up to date
	kvstore     [10]map[string]string
	answerStore map[int64]string //This stores the answers to pass unreliable
	curKeys     map[int]bool
}

func (kv *ShardKV) syncOps(details Op) string {
	// Need to figure a way of modifying to so that the rpc count is low and not to slow
	to := 10 * time.Millisecond
	kv.mu.Lock()
	val, ok := kv.answerStore[details.OpId]
	if ok {
		kv.mu.Unlock()
		return val
	}
	ans := ""
	for {
		seq := kv.syncedTill
		decided, value := kv.px.Status(seq)
		if decided {
			if to > 1*time.Second {
				to = 1 * time.Second
			}
			v := value.(Op)
			ans = ""
			if v.OpType == "Config" {
				// Handle send the kv store to others
				for new_keys, _ := range v.NewConfig {
					if _, ok := kv.curKeys[new_keys]; !ok {
						kv.getkv(new_keys)
					}
				}

				kv.curKeys = v.NewConfig

				// Handle send key error if the config does not have the keys

			} else if v.OpType == "Put" {
				shard := key2shard(v.Key)
				ans = ""
				kv.kvstore[shard][v.Key] = v.Value
			} else if v.OpType == "Put hash" {
				shard := key2shard(v.Key)
				ans = kv.kvstore[shard][v.Key]
				kv.kvstore[shard][v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
				// kv.kvstore[v.Key] = hash(ans + v.Value)
			} else if v.OpType == "Get" {
				shard := key2shard(v.Key)
				ans = kv.kvstore[shard][v.Key]
			}
			kv.answerStore[v.OpId] = ans
			if details.OpId == v.OpId {
				break
			}

			// kv.px.Done(seq)
			seq++
			kv.syncedTill++

		} else {
			details.Seq = seq
			// kv.mu.Unlock()
			kv.px.Start(seq, details)
			time.Sleep(to)
			if to < 10*time.Second {
				r := /* rand.Intn(2) +*/ 2
				to *= time.Duration(r)
			}
			// kv.mu.Lock()
		}
	}
	kv.px.Done(kv.syncedTill)
	kv.syncedTill++
	kv.mu.Unlock()
	return ans
}

func (kv *ShardKV) getkv(key int) {

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// shard := key2shard(args.Key)
	// servingKey, _ := kv.curKeys[shard]
	// if servingKey == false {
	// 	reply.Err = ErrWrongGroup
	// 	return nil
	// }
	operationDetails := new(Op)
	operationDetails.OpType = "Get"
	operationDetails.Key = args.Key
	operationDetails.Value = ""
	operationDetails.OpId = args.Nrand
	reply.Value = kv.syncOps(*operationDetails)
	reply.Err = OK
	// if args.Delete != 0 {
	// kv.mu.Lock()
	// _, ok := kv.answerStore[args.Delete]
	// if ok {
	// delete(kv.answerStore, args.Delete)
	// }
	// kv.mu.Unlock()
	// }
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	// shard := key2shard(args.Key)
	// servingKey, _ := kv.curKeys[shard]
	// if servingKey == false {
	// 	reply.Err = ErrWrongGroup
	// 	return nil
	// }
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
	reply.Err = OK
	// if args.Delete != 0 {
	// 	kv.mu.Lock()
	// 	_, ok := kv.answerStore[args.Delete]
	// 	if ok {
	// 		delete(kv.answerStore, args.Delete)
	// 	}
	// 	kv.mu.Unlock()
	// }
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

	// One problem with the implementation is that there will be several config changes
	// even the new config is known about, we can create an additional map to store this info
	// kv.curKeys =
	new_config := make(map[int]bool)
	config := kv.sm.Query(-1)
	for k, v := range config.Shards {
		if v == kv.gid {
			new_config[k] = true
		}

	}
	kv.mu.Lock()
	// Are the maps equal, if no change config
	eq := reflect.DeepEqual(new_config, kv.curKeys)
	kv.mu.Unlock()
	if eq == false {
		operationDetails := new(Op)
		operationDetails.OpType = "Config"
		operationDetails.OpId = nrand()
		operationDetails.NewConfig = new_config
		kv.syncOps(*operationDetails)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	for k := 0; k < 10; k++ {
		kv.kvstore[k] = make(map[string]string)
	}
	kv.answerStore = make(map[int64]string)
	kv.curKeys = make(map[int]bool)
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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

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
	Seq          int
	OpType       string
	Key          string
	Value        string
	OpId         int64
	NewConfig    [shardmaster.NShards]bool
	SendShardsTo map[int64][]int
	Num          int
	Map          [shardmaster.NShards]map[string]string
	ShardsToCopy []int
	AnswerMap    map[int64]string          //This stores the answers to pass unreliable
	Stale        [shardmaster.NShards]bool //Data is stale, do not use
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
	kvstore     [shardmaster.NShards]map[string]string
	answerStore map[int64]string //This stores the answers to pass unreliable
	curKeys     [shardmaster.NShards]bool
	Groups      map[int64][]string
	new         bool                      //To keep track of newly added server
	stale       [shardmaster.NShards]bool //Data is stale, do not use
	Num         int

	KnownConfig int
}

func (kv *ShardKV) syncOps(details Op) string {
	// Need to figure a way of modifying to so that the rpc count is low and not to slow
	to := 10 * time.Millisecond
	// kv.mu.Lock()
	val, ok := kv.answerStore[details.OpId]
	if ok {
		// kv.mu.Unlock()
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
			if v.OpType == "Recieve" {
				for anskey, ansvalue := range v.AnswerMap {
					kv.answerStore[anskey] = ansvalue
				}
				kv.answerStore[v.OpId] = ans
				for _, shard := range v.ShardsToCopy {
					kv.kvstore[shard] = v.Map[shard]
					kv.stale[shard] = false
				}
				if details.OpType == "Get" ||
					details.OpType == "Put hash" || details.OpType == "Put" {
					shard := key2shard(details.Key)
					if kv.stale[shard] == true {
						return "KeySent"
					}
				}
			} else if v.OpType == "Config" {
				// Handle send the kv store to others
				kv.answerStore[v.OpId] = ans
				if kv.Num < v.Num {
					kv.sendShards(v.SendShardsTo, v.Num, v.OpId)
					kv.curKeys = v.NewConfig
					kv.Num = v.Num
					for i := 0; i < shardmaster.NShards; i++ {
						if kv.stale[i] == false && v.Stale[i] == false {
							kv.stale[i] = false
						} else {
							kv.stale[i] = true
						}
					}

				}

				// Handle send key error if the config does not have the keys

			} else if v.OpType == "Put" {
				shard := key2shard(v.Key)
				ans = ""
				if kv.curKeys[shard] == true || kv.stale[shard] == false {
					kv.kvstore[shard][v.Key] = v.Value
				} else {
					ans = "KeySent"
				}
			} else if v.OpType == "Put hash" {
				shard := key2shard(v.Key)
				if kv.curKeys[shard] == true || kv.stale[shard] == false {
					ans = kv.kvstore[shard][v.Key]
					// t, _ := strconv.Atoi(ans + v.Value)
					// kv.kvstore[shard][v.Key] = strconv.Itoa(int(t))
					kv.kvstore[shard][v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
				} else {
					ans = "KeySent"
				}
			} else if v.OpType == "Get" {
				shard := key2shard(v.Key)
				if kv.curKeys[shard] == true || kv.stale[shard] == false {
					ans = kv.kvstore[shard][v.Key]
					// return "KeySent"
				} else {
					ans = "KeySent"
				}
			}
			if ans != "KeySent" {
				kv.answerStore[v.OpId] = ans
			}
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
				to *= time.Duration(2)
			}
			// kv.mu.Lock()
		}
	}
	// kv.px.Done(kv.syncedTill)
	kv.syncedTill++
	// kv.mu.Unlock()
	return ans
}

func (kv *ShardKV) sendShards(shardsToSend map[int64][]int, Num int, operationId int64) {
	for gid, shards := range shardsToSend {
		go func(shards []int, gid int64) {
			kv.mu.Lock()
			args := &SendArgs{}
			// args.Map = kv.kvstore
			for s := 0; s < shardmaster.NShards; s++ {
				args.Map[s] = make(map[string]string)
				for k, v := range kv.kvstore[s] {
					args.Map[s][k] = v
				}

			}
			args.ShardsToCopy = shards
			args.Num = Num
			args.Nrand = operationId
			// args.AnswerMap = kv.answerStore
			args.AnswerMap = make(map[int64]string)
			for k, v := range kv.answerStore {
				args.AnswerMap[k] = v
			}

			kv.mu.Unlock()
			for {
				var reply GetReply
				ok := false
				for _, server := range kv.Groups[gid] {

					ok = call(server, "ShardKV.RecieveShards", args, &reply)
				}
				if ok && (reply.Err == OK) {
					break
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}(shards, gid)

	}
}

func (kv *ShardKV) RecieveShards(args *SendArgs, reply *SendReply) error {
	//Chane this to ==?
	if kv.Num > args.Num {
		reply.Err = OK
		return nil
	}
	operationDetails := new(Op)
	operationDetails.Map = args.Map
	operationDetails.OpType = "Recieve"
	operationDetails.ShardsToCopy = args.ShardsToCopy
	operationDetails.OpId = args.Nrand
	operationDetails.Num = args.Num
	operationDetails.AnswerMap = args.AnswerMap
	kv.mu.Lock()
	kv.syncOps(*operationDetails)
	kv.mu.Unlock()
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	servingKey := kv.curKeys[shard]
	if servingKey == false || kv.stale[shard] == true {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}
	operationDetails := new(Op)
	operationDetails.OpType = "Get"
	operationDetails.Key = args.Key
	operationDetails.Value = ""
	operationDetails.OpId = args.Nrand
	reply.Value = kv.syncOps(*operationDetails)
	reply.Err = OK
	kv.mu.Unlock()
	if reply.Value == "KeySent" {

		reply.Err = ErrWrongGroup
	}
	if reply.Value == "" {
	}
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
	// Yournn code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	servingKey := kv.curKeys[shard]
	if servingKey == false || kv.stale[shard] == true {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}
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
	kv.mu.Unlock()
	reply.Err = OK
	if reply.PreviousValue == "" {
	}
	if reply.PreviousValue == "KeySent" {
		reply.Err = ErrWrongGroup
	}
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
	var new_config [shardmaster.NShards]bool
	shards_to_send := make(map[int64][]int)
	config := kv.sm.Query(kv.KnownConfig)
	var stale [shardmaster.NShards]bool
	kv.KnownConfig += 1
	kv.mu.Lock()
	if config.Num == 1 && config.Shards[0] == kv.gid {
		for k := 0; k < shardmaster.NShards; k++ {
			kv.stale[k] = false
			kv.curKeys[k] = true
		}
	}
	kv.Groups = config.Groups

	for k, v := range config.Shards {
		// if config.Num < 2 {
		// 	break
		// }
		if v == kv.gid {
			new_config[k] = true
			// if kv.curKeys[k] == false {
			// 	kv.stale[k] = true
			// }
		} else {
			new_config[k] = false
			stale[k] = true

			if kv.curKeys[k] == true {
				shards_to_send[v] = append(shards_to_send[v], k)
			}
		}

	}
	if config.Num >= kv.Num {
		operationDetails := new(Op)
		operationDetails.SendShardsTo = shards_to_send
		operationDetails.OpType = "Config"
		operationDetails.OpId = nrand()
		operationDetails.Num = config.Num
		operationDetails.NewConfig = new_config
		operationDetails.Stale = stale
		kv.syncOps(*operationDetails)
	}
	kv.mu.Unlock()
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
	for k := 0; k < shardmaster.NShards; k++ {
		kv.kvstore[k] = make(map[string]string)
		kv.stale[k] = true
	}
	kv.answerStore = make(map[int64]string)
	kv.Groups = make(map[int64][]string)
	// kv.curKeys = make(map[int]bool)
	kv.syncedTill = 0
	kv.new = true
	kv.KnownConfig = 0
	kv.Num = -1
	// kv.stale = true

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

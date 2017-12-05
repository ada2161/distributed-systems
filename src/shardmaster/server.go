package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	max_seq int
}

type Op struct {
	// Your data here.
	OpID       int64
	new_config Config
	OpType     string
	GID        int64
	Servers    []string
	Shard_no   int
	Query_no   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	new_op := new(Op)
	new_op.OpID = nrand()
	new_op.OpType = "Join"
	new_op.GID = args.GID
	new_op.Servers = args.Servers
	sm.syncOps(*new_op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	new_op := new(Op)
	new_op.OpID = nrand()
	new_op.OpType = "Leave"
	new_op.GID = args.GID
	sm.syncOps(*new_op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	new_op := new(Op)
	new_op.OpID = nrand()
	new_op.OpType = "Move"
	new_op.GID = args.GID
	new_op.Shard_no = args.Shard
	sm.syncOps(*new_op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	new_op := new(Op)
	new_op.OpID = nrand()
	new_op.OpType = "Query"
	new_op.Query_no = args.Num
	sm.syncOps(*new_op)

	if args.Num == -1 || args.Num > (len(sm.configs)-1) {
		args.Num = len(sm.configs) - 1
	}
	reply.Config = sm.configs[args.Num]
	// fmt.Println(sm.configs)

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) syncOps(new_Op Op) {
	// Implementing Join for now
	to := 10 * time.Millisecond
	sm.mu.Lock()
	// fmt.Println(new_Op.OpType, sm.me)

	for {
		cur_config := sm.configs[len(sm.configs)-1]
		seq := sm.max_seq
		decided, value := sm.px.Status(seq)
		if decided {
			v := value.(Op)
			var new_config Config
			new_config.Groups = make(map[int64][]string)
			for key, value := range cur_config.Groups {
				new_config.Groups[key] = value
			}

			new_config.Num = cur_config.Num + 1
			new_config.Shards = cur_config.Shards
			if v.OpType == "Join" {
				// fmt.Println(new_Op.Servers, new_Op.GID)

				new_config.Groups[v.GID] = v.Servers

				new_config = sm.distribute(new_config)
				sm.configs = append(sm.configs, new_config)

			} else if v.OpType == "Leave" {
				delete(new_config.Groups, v.GID)
				new_config = sm.distribute(new_config)
				sm.configs = append(sm.configs, new_config)

			} else if v.OpType == "Move" {
				new_config.Shards[v.Shard_no] = v.GID
				sm.configs = append(sm.configs, new_config)
			} else if v.OpType == "Query" {
				// sm.configs = append(sm.configs, new_config)
			}
			if new_Op.OpID == v.OpID {
				break
			}

			// kv.px.Done(seq)
			sm.max_seq++

		} else {
			sm.px.Start(seq, new_Op)
			time.Sleep(to)
			if to < 10*time.Second {
				r := 2
				to *= time.Duration(r)
			}
		}
	}
	sm.px.Done(sm.max_seq)
	sm.max_seq++
	sm.mu.Unlock()
	return
}

func (sm *ShardMaster) distribute(config Config) Config {
	if len(config.Groups) == 0 {
		return config
	}
	number_of_shards := 10 / len(config.Groups)
	greater_allowed := 10 % len(config.Groups)
	count := make(map[int64]int)
	seq_nu := len(sm.configs) - 1

	// Count number of shards served by a group
	for k, v := range config.Shards {

		// This is used to give back if the group is not present
		if _, ok := config.Groups[v]; ok == false {
			config.Shards[k] = 0
			continue
		}
		if v == 0 {
			continue
		}
		count[v] += 1
	}

	// Remove excess shards
	for k, v := range count {
		if v == 0 || k == 0 {
			continue
		}
		if v == number_of_shards+1 && greater_allowed > 0 {
			greater_allowed--
		} else if v >= number_of_shards+1 {
			remove_shards := v - number_of_shards
			if greater_allowed > 0 {
				remove_shards--
				greater_allowed--
			}
			for idx, _ := range config.Shards {
				if config.Shards[idx] == k {
					config.Shards[idx] = 0
					remove_shards--
				}
				if remove_shards == 0 {
					break
				}

			}
		}
	}
	for k, _ := range config.Groups {
		v := count[k]
		if v < number_of_shards || (v == number_of_shards && greater_allowed > 0) {
			add_shards := number_of_shards - v
			if greater_allowed > 0 {
				add_shards++
				greater_allowed--
			}
			for idx, _ := range sm.configs[seq_nu].Shards {
				if config.Shards[idx] == 0 {
					config.Shards[idx] = k
					add_shards--
				}
				if add_shards == 0 {
					break
				}

			}

		}

	}
	return config

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.max_seq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}

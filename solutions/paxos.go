package paxos

// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "strconv"
import "math/rand"
import "math"
import "reflect"
//import "time"

const DEBUG = false

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	acceptorHistory map[int]*AcceptorState
	numMajority     int
	peerMins        []int
	maxSeq          int
}

/*----------------------------------------------------------------------------*/
/*      RPC/Helper Structs                                                           */
/*----------------------------------------------------------------------------*/

type Error string

const (
	OK           = "OK"
	IDTooSmall   = "IDTooSmall"
    NetworkError = "NetworkError"
)

type PaxosArgs struct {
	Seq    int
	ID     float64
	Value  interface{}
	Caller int
	Min    int
}

type PaxosReply struct {
	Err   Error
	ID    float64
	Value interface{}
	Min   int
    Callee int
}

// Denotes the acceptor state for a given sequence number.
type AcceptorState struct {
	LargestPrepare float64
	LargestAccept  float64
    Value          interface{}
    Decided        bool
}

/*----------------------------------------------------------------------------*/
/*      RPC Handlers                                                          */
/*----------------------------------------------------------------------------*/

// Receiver checks if this pepare is the largest it has seen for the given
// sequence.  If so, it replies OK with the highest number and value the
// receiver has accepted so far.  If not, it replys Error
// "IDTooSmall"
func (px *Paxos) Prepare(args *PaxosArgs, reply *PaxosReply) error {

	px.updatePeerMins(args.Caller, args.Min)

	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Min    = px.peerMins[px.me]
    reply.Callee = px.me

	if DEBUG {
		fmt.Printf("%d received Prepare: {seq %d, val %v, id %.2f}\n", px.me,
			args.Seq, reflect.TypeOf(args.Value), args.ID)
		fmt.Println(px.acceptorHistory)
	}

	seq := args.Seq
	acc := px.acceptorHistory[seq]

	if acc == nil {
		acc = new(AcceptorState)
	}
	if args.ID > acc.LargestPrepare {
		acc.LargestPrepare = args.ID
        px.acceptorHistory[seq] = acc

		reply.ID    = acc.LargestAccept
		reply.Value = acc.Value
		reply.Err   = OK

		if DEBUG {
			fmt.Printf("%d's response: {val %v, id: %.2f}\n", px.me, reflect.TypeOf(reply.Value), reply.ID)
		}
	} else {
		if DEBUG {
			fmt.Printf("%d's response: ID too small\n", px.me)
		}
		reply.Err = IDTooSmall
	}
	return nil
}

// Accept this value if the proposal number is the largest seen so far
func (px *Paxos) Accept(args *PaxosArgs, reply *PaxosReply) error {

	px.updatePeerMins(args.Caller, args.Min)

	px.mu.Lock()
	defer px.mu.Unlock()

    reply.Min    = px.peerMins[px.me]
    reply.Callee = px.me

	if DEBUG {
		fmt.Printf("%d received Accept: {seq %d, val %v, id %.2f}\n", px.me, args.Seq, reflect.TypeOf(args.Value), args.ID)
	}

	reply.ID    = args.ID
	reply.Value = args.Value

	acc := px.acceptorHistory[args.Seq]
	if acc == nil {
        acc = new(AcceptorState)
    }
	if args.ID >= acc.LargestPrepare {
		acc.LargestPrepare = args.ID
		acc.LargestAccept  = args.ID
		acc.Value = args.Value
        px.acceptorHistory[args.Seq] = acc
		reply.Err = OK
	} else {
        reply.Err = IDTooSmall
    }
	return nil
}

// Commit the value of a sequence number
func (px *Paxos) Decide(args *PaxosArgs, reply *PaxosReply) error {

	px.updatePeerMins(args.Caller, args.Min)

	px.mu.Lock()
	defer px.mu.Unlock()

	if DEBUG {
		fmt.Printf("%d received Decide: {seq %d, val %v, id %.2f}\n", px.me, args.Seq, reflect.TypeOf(args.Value), args.ID)
	}

    acc := px.acceptorHistory[args.Seq]
    if acc == nil {
        acc = new(AcceptorState)
    }
    acc.Decided = true
    acc.Value = args.Value
    reply.Err = OK
    px.acceptorHistory[args.Seq] = acc
	return nil
}

/*----------------------------------------------------------------------------*/
/*      Helper Functions/Types                                                */
/*----------------------------------------------------------------------------*/
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// Creates an id number for a given value using serverNum as a decimal
// value.  This allows for a total ordering of values.
func IDNum(num int, serverNum int) float64 {
	numStr := strconv.Itoa(num)
	serverNumStr := strconv.Itoa(serverNum)
	ret, _ := strconv.ParseFloat(numStr+"."+serverNumStr, 64)
	return ret
}

/*----------------------------------------------------------------------------*/
/*      Private Methods                                                       */
/*----------------------------------------------------------------------------*/

// Updates this px instance's peerMins array (the minimum sequence number that
// can be forgotten), then clears the acceptor history of any unneeded entries.
// This function mutates the struct and thus the calling function is responsible
// for locking it.
func (px *Paxos) updatePeerMins(peer int, min int) {

    px.mu.Lock()
    defer px.mu.Unlock()

	currMin := px.peerMins[peer]
	if currMin < min {

		px.peerMins[peer] = min

        globalMin := px.Min()
        completed := make([]int, 0)
        for i := range px.acceptorHistory {
            if i <= globalMin {
                completed = append(completed, i)
            }
        }
        for _, i := range completed {
            delete(px.acceptorHistory, i)
        }
    }
}

// Handles the entire Paxos propose phase by
// 1: sending request(seq, id) to all peers
// 2: sending accept(seq, {value, id}) to all peers
// 3: sending commit(seq, {value, id}) to all peers
// The sequence number remains the same, but the value may change throughout the
// proposal process, depending on whether or not other peers have accepted a
// value with higher id for this sequence.  With the exception of FLP scenarios,
// propose should always return.
func (px *Paxos) propose(value interface{}, seq int) {

	if DEBUG {
		fmt.Printf("Host %d beginning proposal: {seq %d, val %v}\n", px.me, seq, reflect.TypeOf(value))
	}

	args := new(PaxosArgs)
	args.Value  = value
	args.Seq    = seq
	args.Caller = px.me

	for id := IDNum(1, px.me); true; id++ {
		// always propose with higher id then all seen so far
        if px.dead {
            return
        }

		if DEBUG {
			fmt.Printf("Proposing Prepare {seq %d, val %v, id %.2f} \n", seq, reflect.TypeOf(value), id)
		}

		// get ready to send rpcs
		args.ID   = id
		args.Min  = px.peerMins[px.me]
        replyChan := make(chan *PaxosReply)

		// Step 1: send prepare to all peers and record all responses
		replies := make([]*PaxosReply, 0)
		for i, p := range px.peers {

			reply := new(PaxosReply)

            go func(i int, p string, args *PaxosArgs, reply *PaxosReply) {
                if i == px.me {
                    px.Prepare(args, reply)
                } else {
                    ok := call(p, "Paxos.Prepare", args, reply)
                    if !ok {
                        reply.Err = NetworkError
                    }
                }
                replyChan <- reply
            }(i, p, args, reply)
		}

        numOK := 0
        for numReplies := 0; numReplies < len(px.peers); numReplies++  {

            reply := <-replyChan
            replies = append(replies, reply)

            if reply.Err == OK {
                numOK++
            }
        }

		// Step 2: send accept to all peers
		if numOK >= px.numMajority {

			// If a peer has already accepted, we must accept the same value
            maxNum := -1.0
            var maxVal interface{}
            for _, reply := range replies {
                if reply.ID > maxNum {
                    maxNum = reply.ID
                    maxVal = reply.Value
                }
            }
			if maxVal != nil {
				args.Value = maxVal
			}

			if DEBUG {
				fmt.Printf("Proposing Accept: {seq %d, val %v}\n", args.Seq, reflect.TypeOf(args.Value))
			}

            replyChan := make(chan *PaxosReply)
            replies := make([]*PaxosReply, 0)
			for i, p := range px.peers {

				reply := new(PaxosReply)

                go func(i int, p string, args *PaxosArgs, reply *PaxosReply) {
                    if i == px.me {
                        px.Accept(args, reply)
                    } else {
                        ok := call(p, "Paxos.Accept", args, reply)
                        if !ok {
                            reply.Err = NetworkError
                        }
                    }
                    replyChan <- reply
                }(i, p, args, reply)
			}

            numOK   := 0
            for numReplies := 0; numReplies < len(px.peers); numReplies++ {
                reply := <-replyChan
                replies = append(replies, reply)
                if reply.Err == OK {
                    numOK++
                }
            }

			// majority response means we can commit the value
			if numOK >= px.numMajority {
				break
			}
		    id = math.Max(id, maxNum) // next round id should be highest yet
		}
	} // end prepare/accept for loop

	// Step 3: commit the value
	args.Min = px.peerMins[px.me]
	for i, p := range px.peers {

        reply := new(PaxosReply)
        if i == px.me {
            px.Decide(args, reply)
        } else {
            call(p, "Paxos.Decide", args, reply)
        }
	}
}

/*----------------------------------------------------------------------------*/
/*      Public Methods                                                        */
/*----------------------------------------------------------------------------*/

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.propose(v, seq)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
    px.updatePeerMins(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	for seq := range px.acceptorHistory {
		if seq > px.maxSeq {
			px.maxSeq = seq
		}
	}
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.

	globalMin := px.peerMins[0]
	for _, num := range px.peerMins {
		if num < globalMin {
			globalMin = num
		}
	}
	return globalMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.

	px.mu.Lock()
	defer px.mu.Unlock()

	acc := px.acceptorHistory[seq]
	if acc == nil {
		return false, nil
	} else {
		return acc.Decided , acc.Value
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.numMajority = (len(peers) / 2) + 1
	px.acceptorHistory = make(map[int]*AcceptorState)
	px.peerMins = make([]int, len(peers))
	for i := range px.peerMins {
		px.peerMins[i] = -1
	}
	px.maxSeq = -1

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

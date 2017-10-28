package paxos

//
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

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	majority int
	stateMap map[int]Info
	max      int
	minDone  int
	DoneMap  map[int]int
}

type Info struct {
	proposalNumberToBeGiven ProposalNumber
	highestNumberSeen       ProposalNumber
	highestNumberAccepted   ProposalNumber
	valueOfHighestAccepted  interface{}
	DecidedValue            interface{}
	//Maintain boolean for decided or not?
}

type ProposalNumber struct {
	UniqeNumber int
	Sender      int
}

type ProposalArgs struct {
	Seq        int
	ProposalNo ProposalNumber
	MinDone    int
	Sender     int
}

type ProposalReply struct {
	ProposeOk     bool
	ProposeNumber ProposalNumber
	ProposeValue  interface{}
}

type AcceptArgs struct {
	AcceptNo    ProposalNumber
	AcceptValue interface{}
	AcceptSeq   int
}

type AcceptReply struct {
	AcceptedNo ProposalNumber
	AcceptOk   bool
}

type DecisionReply struct {
	DoneOk bool
}

type DecisionArgs struct {
	DecidedValue interface{}
	Seq          int
}

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
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func (px *Paxos) Propose(args *ProposalArgs, reply *ProposalReply) error {
	px.mu.Lock()
	seqInfo, ok := px.stateMap[args.Seq]
	if ok == false {
		seqInfo := new(Info)
		seqInfo.proposalNumberToBeGiven.UniqeNumber = 0
		seqInfo.proposalNumberToBeGiven.Sender = 0
		seqInfo.highestNumberSeen.UniqeNumber = 0
		seqInfo.highestNumberSeen.Sender = 0
		seqInfo.highestNumberAccepted.UniqeNumber = 0
		seqInfo.highestNumberAccepted.Sender = 0
		seqInfo.valueOfHighestAccepted = nil
		seqInfo.DecidedValue = nil
	}
	//Logic for forgetting
	px.DoneMap[args.Sender] = args.MinDone
	minDone := px.DoneMap[0]
	for _, dones := range px.DoneMap {

		if minDone > dones {
			minDone = dones
		}
	}
	if minDone > px.minDone {
		px.Forget(minDone)
	}
	if isStrictlyGreater(args.ProposalNo, seqInfo.highestNumberSeen) {
		seqInfo.highestNumberSeen = args.ProposalNo
		seqInfo.proposalNumberToBeGiven = args.ProposalNo
		reply.ProposeNumber = seqInfo.highestNumberAccepted
		reply.ProposeValue = seqInfo.valueOfHighestAccepted
		reply.ProposeOk = true
		px.stateMap[args.Seq] = seqInfo
	} else {
		reply.ProposeOk = false
		reply.ProposeNumber = seqInfo.highestNumberAccepted
	}
	px.mu.Unlock()
	return nil
}

func isGreater(a ProposalNumber, b ProposalNumber) bool {

	if a.UniqeNumber > b.UniqeNumber {
		return true
	}
	if a.UniqeNumber == b.UniqeNumber && a.Sender >= b.Sender {
		return true
	}
	return false

}

func isStrictlyGreater(a ProposalNumber, b ProposalNumber) bool {

	if a.UniqeNumber > b.UniqeNumber {
		return true
	}
	if a.UniqeNumber == b.UniqeNumber && a.Sender > b.Sender {
		return true
	}
	return false

}

func (px *Paxos) Forget(ForgetTill int) {
	for key := range px.stateMap {
		if key < ForgetTill {
			delete(px.stateMap, key)
		}
	}

	px.minDone = ForgetTill
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	seqInfo, ok := px.stateMap[args.AcceptSeq]
	if ok == false {
		seqInfo := new(Info)
		seqInfo.proposalNumberToBeGiven.UniqeNumber = 0
		seqInfo.proposalNumberToBeGiven.Sender = 0
		seqInfo.highestNumberSeen.UniqeNumber = 0
		seqInfo.highestNumberSeen.Sender = 0
		seqInfo.highestNumberAccepted.UniqeNumber = 0
		seqInfo.highestNumberAccepted.Sender = 0
		seqInfo.valueOfHighestAccepted = nil
		seqInfo.DecidedValue = nil
	}
	if isGreater(args.AcceptNo, seqInfo.highestNumberSeen) {
		seqInfo.highestNumberAccepted = args.AcceptNo
		seqInfo.valueOfHighestAccepted = args.AcceptValue
		// if seqInfo.highestSeq < args.AcceptSeq {
		// 	seqInfo.highestSeq = args.AcceptSeq
		// }
		px.stateMap[args.AcceptSeq] = seqInfo
		reply.AcceptedNo = args.AcceptNo
		reply.AcceptOk = true
	} else {
		reply.AcceptOk = false
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Decide(args *DecisionArgs, reply *DecisionReply) error {
	px.mu.Lock()
	seqInfo, ok := px.stateMap[args.Seq]
	if ok == false {
		seqInfo := new(Info)
		seqInfo.proposalNumberToBeGiven.UniqeNumber = 0
		seqInfo.proposalNumberToBeGiven.Sender = 0
		seqInfo.highestNumberSeen.UniqeNumber = 0
		seqInfo.highestNumberSeen.Sender = 0
		seqInfo.highestNumberAccepted.UniqeNumber = 0
		seqInfo.highestNumberAccepted.Sender = 0
		seqInfo.valueOfHighestAccepted = nil
		seqInfo.DecidedValue = nil
	}
	seqInfo.DecidedValue = args.DecidedValue
	px.stateMap[args.Seq] = seqInfo
	if args.Seq > px.max {
		px.max = args.Seq
	}
	reply.DoneOk = true
	px.mu.Unlock()
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//Propose Here
	// sequenceDone, _ := px.Status(seq)
	// if sequenceDone == true {
	// 	return
	// }

	px.mu.Lock()
	if seq < px.minDone {
		return
	}
	var args ProposalArgs
	seqInfo, ok := px.stateMap[seq]
	if ok == false {
		seqInfo := new(Info)
		seqInfo.proposalNumberToBeGiven.UniqeNumber = 0
		seqInfo.proposalNumberToBeGiven.Sender = 0
		seqInfo.highestNumberSeen.UniqeNumber = 0
		seqInfo.highestNumberSeen.Sender = 0
		seqInfo.highestNumberAccepted.UniqeNumber = 0
		seqInfo.highestNumberAccepted.Sender = 0
		seqInfo.valueOfHighestAccepted = nil
		seqInfo.DecidedValue = nil
	}
	// incSeqBy := rand.Intn(10)
	incSeqBy := 1
	seqInfo.proposalNumberToBeGiven.UniqeNumber += incSeqBy
	seqInfo.proposalNumberToBeGiven.Sender = px.me
	args.ProposalNo = seqInfo.proposalNumberToBeGiven
	args.MinDone = px.DoneMap[px.me]
	args.Sender = px.me
	px.mu.Unlock()
	go func() {
		var maxValue interface{}
		var maxNumber ProposalNumber
		maxNumber.Sender = 0
		maxNumber.UniqeNumber = 0
		var proposalAccepted []string
		var maxValueToRetry ProposalNumber
		maxValueToRetry.Sender = 0
		maxValueToRetry.UniqeNumber = 0
		for id, peer := range px.peers {
			var reply ProposalReply
			//Can make below call more concurrent, using channels etc
			args.Seq = seq
			ok := false
			if px.me == id {
				px.Propose(&args, &reply)
				ok = true
			} else {
				ok = call(peer, "Paxos.Propose", args, &reply)
				if isGreater(reply.ProposeNumber, maxValueToRetry) {
					maxValueToRetry = reply.ProposeNumber
				}
			}
			if ok && reply.ProposeOk == true {
				proposalAccepted = append(proposalAccepted, peer)
				if isGreater(reply.ProposeNumber, maxNumber) {
					maxNumber = reply.ProposeNumber
					maxValue = reply.ProposeValue
				}
			}

		}

		//Accept here
		if len(proposalAccepted) > px.majority {
			if maxValue == nil {
				maxValue = v
			}
			var args AcceptArgs
			args.AcceptNo = seqInfo.proposalNumberToBeGiven
			args.AcceptValue = maxValue
			args.AcceptSeq = seq
			accepted := 0
			for id, peer := range px.peers {
				var reply AcceptReply
				ok := false
				if px.me == id {
					px.Accept(&args, &reply)
					ok = true
				} else {
					ok = call(peer, "Paxos.Accept", args, &reply)
				}
				if ok && reply.AcceptOk == true {
					accepted++
				}

			}
			//Decided the value
			if accepted > px.majority {
				var args DecisionArgs
				args.DecidedValue = maxValue
				args.Seq = seq
				for id, peer := range px.peers {
					go px.SendDone(args, peer, id)
				}

			} else {
				px.mu.Lock()
				seqInfo, _ := px.stateMap[seq]
				if isGreater(maxValueToRetry, seqInfo.proposalNumberToBeGiven) {
					seqInfo.proposalNumberToBeGiven = maxValueToRetry
				}
				px.stateMap[seq] = seqInfo
				r := rand.Intn(100)
				time.Sleep(time.Duration(r) * time.Microsecond)
				px.mu.Unlock()
				go px.Start(seq, v)
			}

		} else {
			px.mu.Lock()
			seqInfo, _ := px.stateMap[seq]
			if isGreater(maxValueToRetry, seqInfo.proposalNumberToBeGiven) {
				seqInfo.proposalNumberToBeGiven = maxValueToRetry
			}
			px.stateMap[seq] = seqInfo
			r := rand.Intn(100)
			time.Sleep(time.Duration(r) * time.Microsecond)
			px.mu.Unlock()
			go px.Start(seq, v)
		}

	}()
}

func (px *Paxos) SendDone(args DecisionArgs, peer string, id int) {
	var reply DecisionReply
	ok := false
	if px.me == id {
		px.Decide(&args, &reply)
		ok = true
	} else {
		ok = call(peer, "Paxos.Decide", args, &reply)
	}

	if ok == false {
		r := rand.Intn(100)
		time.Sleep(time.Duration(r) * time.Microsecond)
		go px.SendDone(args, peer, id)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.DoneMap[px.me] = seq + 1

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
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
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return (px.minDone)
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
	if seq < px.minDone {
		return false, nil
	}
	val, ok := px.stateMap[seq]
	px.mu.Unlock()
	if ok {
		if val.DecidedValue != nil {
			return true, val.DecidedValue
		}
	}
	return false, nil
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

	// Your initialization code here.
	px.majority = len(peers) / 2
	px.stateMap = make(map[int]Info)
	px.minDone = 0
	px.DoneMap = make(map[int]int)

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

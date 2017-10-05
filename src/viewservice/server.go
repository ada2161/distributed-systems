
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  //To keep the most recent communication time
  timeMap map[string]int64
  //currentView is the view which has been acknowledged by the primary
  currentView View
  //viewServiceView is the viewservices view and will become the view for the primary once the primary acknowledges it is in a view 1 behind the new one 
  viewServiceView View
  IsConsistent bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  //TODO Handle locks
  //When a ping is recieved we do the following:
  //store the time it is recieved for the server by
  // Checking if its the first ping or not and update the map accordingly
  vs.mu.Lock()
  defer vs.mu.Unlock()
  now := time.Now()

  vs.timeMap[args.Me] = now.UnixNano()
  // Doing it for the first time only and return instantly to avoid complications
  if vs.currentView.Primary == "" && vs.currentView.Viewnum == 0 {
	 vs.currentView.Primary = args.Me
	 vs.currentView.Viewnum = vs.currentView.Viewnum + 1 
     vs.IsConsistent = false
	 vs.viewServiceView = vs.currentView
	 reply.View = vs.viewServiceView
	 return nil
  }

   if vs.currentView.Primary == args.Me && args.Viewnum == 0 {
	 vs.makeNewPrimary()
	 // reply.View = vs.viewServiceView
	 // return nil
   }

  // //TODO Check if not consistent and view is different 
  if vs.currentView.Primary == args.Me && (vs.IsConsistent==false  && args.Viewnum == vs.currentView.Viewnum) {
	  vs.IsConsistent = true
  }
// //If backup can be assigned
   if vs.viewServiceView.Backup == "" && vs.viewServiceView.Primary != args.Me {
	  vs.viewServiceView.Backup = args.Me
	  vs.viewServiceView.Viewnum = vs.viewServiceView.Viewnum + 1 
   }

   if vs.IsConsistent == true && vs.currentView!=vs.viewServiceView{
	   vs.currentView = vs.viewServiceView
	   vs.IsConsistent = false
   }


   reply.View = vs.currentView

   return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  reply.View = vs.currentView 
  return nil
}

func (vs *ViewServer) makeNewBackup() {
	// Your code here.
	//Backup might have failed
	vs.viewServiceView.Backup = ""
	for server := range vs.timeMap {
	 	if server != vs.currentView.Primary{
	 		vs.currentView.Backup = server
	 		vs.IsConsistent = false
	 		vs.viewServiceView = vs.currentView
	 	}
	}
	if vs.IsConsistent == true{
	   vs.currentView = vs.viewServiceView	
	}
	vs.viewServiceView.Viewnum = vs.viewServiceView.Viewnum + 1 
	vs.IsConsistent = false
}
 

func (vs *ViewServer) makeNewPrimary() {
  // Your code here.
  //If primary fails we can skip views or we can go to the next state in which a 
  //Backup might have failed
  vs.currentView.Primary = vs.currentView.Backup
  vs.currentView.Backup = ""
  vs.currentView.Viewnum = vs.currentView.Viewnum + 1 
  vs.IsConsistent = false
  vs.viewServiceView = vs.currentView
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly. It should also update the map of available workers in the same way.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for server, lastPing := range vs.timeMap {
		currentTime := time.Now()
		currentT := currentTime.UnixNano()
		elapsedTimeInMilli := (currentT-lastPing) 
		//Check if conversion works
		if(elapsedTimeInMilli > (DeadPings * int64(PingInterval))){
			delete(vs.timeMap, server)
			if(server == vs.currentView.Primary && vs.IsConsistent == true){
				vs.makeNewPrimary()
			}else if(server == vs.currentView.Backup){
				vs.makeNewBackup()
			}
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.mu.Lock()
  defer vs.mu.Unlock()
  vs.me = me
  // Your vs.* initializations here.

  vs.timeMap = make(map[string]int64)
  vs.currentView.Primary = ""
  vs.currentView.Backup = ""
  vs.currentView.Viewnum = 0
  vs.viewServiceView = vs.currentView
  vs.IsConsistent = true
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}

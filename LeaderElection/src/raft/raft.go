package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // to know what is current Term going on
	//votedFor        int // If set then don't vote again.
	electionTimeOut *time.Timer
	voteCountMap    map[int]int
	//log         []string
	//commitIndex int
	//lastApplied int
	//nextIndex   []int
	//matchIndex  []int
	heartbeatTimeout *time.Timer
	serverState      int // 0 for follower, 1 for candidate and 2 for leader
	voteMap          map[int]int
}

type AppendEntriesArgs struct {
	Term     int //leader's term
	LeaderId int // for redirecting requests
	// not needed for assignment 3
	PrevLogIndex string
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm and also if term is greater than equal to currentTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	term = rf.currentTerm
	//isleader = rf.serverState == 2
	//fmt.Printf("Server is %d term is %d and server state is %d", rf.me, term, rf.serverState)
	//fmt.Println()
	// Your code here.
	return term, rf.serverState == 2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// Change state function
func (rf *Raft) changeState(state int) {
	if rf.serverState == state {
		return
	}
	//fmt.Printf("Server %d is changing to state %d for term %d", rf.me, state, rf.currentTerm)
	//fmt.Println()
	if state == 0 {
		rf.serverState = 0
		rf.voteMap[rf.currentTerm] = -1
		rf.voteCountMap[rf.currentTerm] = -1
		rf.heartbeatTimeout.Stop()
		rf.electionTimeOut.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	} else if state == 1 {
		rf.serverState = 1
		//rf.heartbeatTimeout.Stop()
		//start election
		rf.Election()
	} else {
		// send Heartbeats to the server
		rf.serverState = 2
		rf.electionTimeOut.Stop()
		//fmt.Println("Are we coming here?")
		//fmt.Printf("Server %d is the leader", rf.me)
		//rf.GetState()
		//fmt.Println()
		rf.heartbeatTimeout.Reset(time.Duration(rand.Intn(50)+20) * time.Millisecond)
		//fmt.Printf("Server %d heartbeat's timeout reset happended", rf.me)
		//fmt.Println()
		rf.sendHeartbeat()
	}
}

func (rf *Raft) Election() {
	if rf.serverState != 1 {
		return
	}
	//fmt.Printf("Is the election even starting? Candidate is %d", rf.me)
	//fmt.Println()
	rf.electionTimeOut.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	rf.currentTerm += 1
	//fmt.Printf("Term incremented in Election to %d", rf.currentTerm)
	//fmt.Println()
	rf.voteMap[rf.currentTerm] = rf.me
	rf.voteCountMap[rf.currentTerm] = 1
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			go func(peer int) {

				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: -1, LastLogTerm: -1}
				rep := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, args, &rep)
				if ok {
					//fmt.Println("Do I get a response from RequestVote RPC")
					//fmt.Printf("rep.VoteGranted is %v", rep.VoteGranted)
					//fmt.Println()
					if rep.Term > rf.currentTerm {
						//fmt.Printf("server %d is going down", args.CandidateId)
						//fmt.Println()
						//fmt.Println("Leader down")
						//fmt.Println("rep.Term > rf.currentTerm")
						rf.changeState(0)
						return
					}
					if rep.VoteGranted {
						rf.voteCountMap[rf.currentTerm] += 1
						//fmt.Printf("Number of votes %d in term %d for server %d", rf.voteCountMap[rf.currentTerm], rf.currentTerm, rf.me)
						//fmt.Println()
						majorityVotes := int(len(rf.peers)/2) + 1
						//fmt.Printf("Majority votes = %d and votes Obtained ", majorityVotes)
						//fmt.Println()
						if rf.voteCountMap[rf.currentTerm] >= majorityVotes {
							//fmt.Println("Idhar toh aa raha hai kya??")
							rf.changeState(2)
							//fmt.Printf("Server %d is converted to leader", rf.me)
							//fmt.Println()
						}
					}
				}
			}(peer)
		}
		//if rf.serverState != 2 {
		//	fmt.Println("Starting Election again")
		//	rf.Election()
		//}
	}
}

func (rf *Raft) sendHeartbeat() {
	if rf.serverState != 2 {
		return
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			go func(peer int) {
				rep := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, args, &rep)
				if ok {
					if !rep.Success {
						rf.currentTerm = rep.Term
						rf.changeState(0)
					}
				} else {
					//fmt.Println("Heartbeat got no reply")
				}
			}(peer)
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//fmt.Printf("server %d sending requestVote to server %d", args.CandidateId, rf.me)
	//fmt.Println()
	if args.Term < rf.currentTerm {
		//fmt.Printf("Candidate %d has lower term than server %d", args.CandidateId, rf.me)
		//fmt.Println()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// change state
		delete(rf.voteMap, rf.currentTerm)
		delete(rf.voteCountMap, rf.currentTerm)
		rf.changeState(0)
	}
	if _, ok := rf.voteMap[rf.currentTerm]; !ok {
		//fmt.Printf("Server %d has voted for server %d", rf.me, args.CandidateId)
		//fmt.Println()
		rf.voteMap[rf.currentTerm] = args.CandidateId
		reply.VoteGranted = true
	} else {
		if rf.voteMap[rf.currentTerm] == args.CandidateId {
			reply.VoteGranted = true
		} else {
			// fmt.Printf("Server %d has already voted for someone else", rf.me)
			// fmt.Println()
			reply.VoteGranted = false
		}
	}
}

// function for AppendEntries
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.changeState(0)
	reply.Success = true
	reply.Term = rf.currentTerm
	//fmt.Printf("Server %d election timeout resetting after AppendEntriesRPC", rf.me)
	rf.electionTimeOut.Reset((time.Duration(rand.Intn(150)) + 150) * time.Millisecond)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("Server %d sending heartbeat to server %d", args.LeaderId, server)
	//fmt.Println()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	//term := -1
	//isLeader := true

	return index, rf.currentTerm, rf.serverState == 2
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) timer() {

	for {
		select {
		case <-rf.electionTimeOut.C:
			{
				//fmt.Printf("Server %d has a election timeOut", rf.me)
				//fmt.Println()
				rf.changeState(1)
				//rf.currentTerm += 1
				//fmt.Printf("Term incremented in timer to %d", rf.currentTerm)
				//fmt.Println()
				rf.Election()
			}
		case <-rf.heartbeatTimeout.C:
			{
				//fmt.Printf("Server %d has a heartbeat timeout", rf.me)
				//fmt.Println()
				if rf.serverState == 2 {
					//rf.electionTimeOut.Stop()
					//fmt.Printf("Now is the server %d sending a heartbeat", rf.me)
					//fmt.Println()
					rf.sendHeartbeat()
					rf.heartbeatTimeout.Reset(time.Duration(rand.Intn(10)+20) * time.Millisecond)
				}
			}
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	//rf.votedFor = 0
	rf.electionTimeOut = time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	rf.heartbeatTimeout = time.NewTimer(time.Duration(rand.Intn(50)+20) * time.Millisecond)
	//rf.heartbeatTimeout.Stop()
	rf.serverState = 0
	rf.voteMap = make(map[int]int)
	rf.voteCountMap = make(map[int]int)
	// initialize from state persisted before a crash
	go rf.timer()
	rf.readPersist(persister.ReadRaftState())

	return rf
}

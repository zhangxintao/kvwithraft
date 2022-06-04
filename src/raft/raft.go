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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	ElectionTimeoutMin = 400
	ElectionTimeoutMax = 800
	HeartbeatInterval  = 100
	Follower           = "Follower"
	Candidate          = "Candidate"
	Leader             = "Leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	heartbeatLastSeen time.Time
	state             string
}

type LogEntry struct {
	command interface{}
	term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	DPrintf("S%d - GetState term:%v, state:%v", rf.me, rf.currentTerm, rf.state)
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("S%d - receiving append entries %v", rf.me, args)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		DPrintf("S%d - args.Term:%v < rf.currentTerm:%v", rf.me, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if rf.state == Leader {
		if args.Term > rf.currentTerm {
			DPrintf("S%d - Moving from Leader to Follower", rf.me)
			rf.state = Follower
		}
	}
	rf.heartbeatLastSeen = time.Now()
	rf.currentTerm = args.Term
	if rf.state == Candidate {
		DPrintf("S%d - Moving from Candidate to Follower", rf.me)
		rf.state = Follower
		rf.votedFor = -1
	}
	rf.mu.Unlock()

	if len(args.Entries) == 0 {
		// it is a heartbeat
		DPrintf("S%d - processing heartbeat", rf.me)
	}
}

// only leader should involke it.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("S%d - receiving vote request from S%v, votedFor:%v", rf.me, args.CandidateId, rf.votedFor)
	reply.VoteGranted = false

	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if rf.state == Candidate {
		DPrintf("S%d - is Candidate, eligible for vote", rf.me)
		if args.Term > rf.currentTerm {
			DPrintf("S%d - requestors' term is higher, grant vote", rf.me)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
		} else if args.Term == rf.currentTerm {
			DPrintf("S%d - requestors' term is equal, checking", rf.me)
			lastLogIndex := len(rf.log) - 1
			if rf.votedFor == -1 && args.LastLogIndex >= lastLogIndex {
				DPrintf("S%d - requestors' is least-update-to-date, granting", rf.me)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		}
	} else {
		DPrintf("S%d - is %v, no eligible for vote", rf.me, rf.state)
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, c chan bool) bool {
	DPrintf("S%d - asking for vote for %v from S%d", rf.me, args, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	c <- reply.VoteGranted
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		//DPrintf("S%d electionTicker", rf.me)

		rand.Seed(time.Now().UnixNano())
		electionTimeout := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin+1) + ElectionTimeoutMin
		DPrintf("S%d to sleep for %v ms", rf.me, electionTimeout)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		DPrintf("S%d executing electionTicker, with %d peers", rf.me, len(rf.peers))
		rf.mu.Lock()
		if rf.state == Follower || rf.state == Candidate {
			DPrintf("S%d is %v", rf.me, rf.state)
			duration := time.Now().Sub(rf.heartbeatLastSeen).Milliseconds()
			DPrintf("S%d - %v ms passed since last heartbeat", rf.me, duration)
			if duration > ElectionTimeoutMin {
				DPrintf("S%d - starting election", rf.me)
				DPrintf("S%d - got lock", rf.me)
				rf.state = Candidate
				rf.votedFor = -1
				voteCount := 1

				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						term := rf.currentTerm + 1
						candidateId := rf.me
						lastLogIndex := len(rf.log) - 1
						lastLogTerm := rf.log[lastLogIndex].term
						args := RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
						reply := RequestVoteReply{}
						ch := make(chan bool)
						go rf.sendRequestVote(i, &args, &reply, ch)
						select {
						case voted := <-ch:
							if voted {
								voteCount++
							}
						case <-time.After(100 * time.Millisecond):
							DPrintf("S%d - timeout - asking for vote for %v from S%d", rf.me, args, i)
						}
					}
				}
				if voteCount*2 > len(rf.peers) {
					DPrintf("S%d - become Leader", rf.me)
					rf.state = Leader
					rf.currentTerm++
					rf.votedFor = rf.me
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) hearbeatTicker() {
	for rf.killed() == false {
		//DPrintf("S%d hearbeatTicker", rf.me)

		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			heartbeat := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					DPrintf("S%d - sending heartbeat to S%d", rf.me, i)
					ch := make(chan bool)
					go rf.sendAppendEntries(i, &heartbeat, &AppendEntriesReply{}, ch)
					select {
					case <-ch:
						DPrintf("S%d - heartbeat response from S%d", rf.me, i)
					case <-time.After(80 * time.Millisecond):
						DPrintf("S%d - heartbeat to S%d timeout", rf.me, i)
					}
				}
			}
		} else {
			rf.mu.Unlock()
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{term: 0}}

	rf.state = Follower
	rf.heartbeatLastSeen = time.Now()

	DPrintf("S%d - staring with config %v", rf.me, rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.hearbeatTicker()

	return rf
}

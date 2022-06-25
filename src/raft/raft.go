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
	"github.com/google/uuid"
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

	// Persitent state on all servers
	// updated before respoding to RPCs
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	// for internal states
	heartbeatLastSeen time.Time
	state             string
	votesReceived     int
}

type LogEntry struct {
	Command interface{}
	Term    int
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	requestId := uuid.New()
	DPrintf("S%d - %v - receiving append entries from S%v", rf.me, requestId, args.LeaderId)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("S%d - %v - append term out-of-date from S%v", rf.me, requestId, args.LeaderId)
		rf.mu.Unlock()
		return
	}

	// if the term in the AppendEntries arguments is outdated, you should not reset your timer
	rf.resetElectionTimer()

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	if args.PrevLogIndex > -1 {
		if !rf.checkPrevEligibility(args.PrevLogIndex, args.PrevLogTerm) {
			DPrintf("S%d - %v - failed on prevLogIndex and prevLogTerm check, from S%v", rf.me, requestId, args.LeaderId)
			rf.mu.Unlock()
			return
		}
	}

	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.state = Follower
	}

	if len(args.Entries) > 0 {
		DPrintf("S%d - %v - processing non heartbeat request, from S%v", rf.me, requestId, args.LeaderId)
		rf.processExisting()
		rf.processCommit()
		DPrintf("S%d - %v - processed non heartbeat request, from S%v", rf.me, requestId, args.LeaderId)
		rf.mu.Unlock()
		return
	}

	DPrintf("S%d - %v - processed heartbeat request, from S%v", rf.me, requestId, args.LeaderId)
	rf.mu.Unlock()
	//a.  If an existing entry conflicts with a new one
	// delete the existing entry and all that follow it
	//b. Append any new entries not already in the log
	/*
		lastIndex := len(rf.log) - 1
		startIndex := args.PrevLogIndex + 1
		entryIndex := 0
		conflictIndex := -1
		for entryIndex < len(args.Entries) && startIndex <= lastIndex {
			if args.Entries[entryIndex].term != rf.log[startIndex].term {
				conflictIndex = startIndex
				break
			}
			startIndex++
			entryIndex++
		}
		if conflictIndex != -1 {
			DPrintf("S%d - found conflict at index:%v, payload: %v", rf.me, conflictIndex, args)
			rf.log = rf.log[0:conflictIndex]
		}
		if entryIndex < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[entryIndex:]...)
		}

		// If leaderCommit > commitIndex, set CommitIndex=min(LeaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
			if args.LeaderCommit < lastNewEntryIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastNewEntryIndex
			}
		}

		reply.Success = true
		rf.mu.Unlock()
	*/
}

func (rf *Raft) checkPrevEligibility(prevLogIndex int, prevLogTerm int) bool {
	if len(rf.log) <= prevLogIndex || rf.log[prevLogIndex].Term != prevLogTerm {
		return false
	}

	return true
}

func (rf *Raft) processExisting() {

}

func (rf *Raft) processCommit() {

}

// only leader should involke it.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	requestId := uuid.New()

	rf.mu.Lock()
	DPrintf("S%d - %v - receiving vote request from S%v, current term:%v", rf.me, requestId, args.CandidateId, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		DPrintf("S%d - %v - term no up-to-date, rejecting vote request from S%v", rf.me, requestId, args.CandidateId)
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		DPrintf("S%d - %v - same term, voted before, rejecting vote request from S%v", rf.me, requestId, args.CandidateId)
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
	}

	DPrintf("S%d - %v - checking up-to-date for vote request from S%v", rf.me, requestId, args.CandidateId)
	if rf.leastUpToDateAsReceiver(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("S%d - %v - grant vote request from S%v", rf.me, requestId, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.resetElectionTimer()
	}
	rf.mu.Unlock()
}

func (rf *Raft) leastUpToDateAsReceiver(lastLogIndex int, lastLogTerm int) bool {
	// if the logs have last entries with different terms, then the log with the later term is more up-to-date
	// if the logs end with the same term, then whichever log is longer is more up-to-date
	if len(rf.log) == 0 {
		return true
	}

	latestTerm := rf.log[len(rf.log)-1].Term
	if latestTerm < lastLogTerm {
		return true
	}

	if latestTerm == lastLogTerm && lastLogIndex >= len(rf.log)-1 {
		return true
	}

	return false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("S%d - asking for vote for %v from S%d", rf.me, args, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	if rf.state != Leader {
		isLeader = false
	} else {
		rf.mu.Lock()
		entry := LogEntry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, entry)
		index = len(rf.log)
		term = rf.currentTerm
		rf.mu.Unlock()
	}

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
		time.Sleep(10 * time.Microsecond)

		rand.Seed(time.Now().UnixNano())
		electionTimeout := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin+1) + ElectionTimeoutMin
		DPrintf("S%d to sleep for %v ms", rf.me, electionTimeout)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Follower {
			DPrintf("S%d executing electionTicker as %v", rf.me, rf.state)
			if rf.electionTimeoutElapse(electionTimeout) {
				DPrintf("S%d election timeout elpase in Follower, transform to Candidate", rf.me)
				rf.state = Candidate
				DPrintf("S%d start election, on conversion to candidate", rf.me)
				rf.startElection()
			}
			DPrintf("S%d end executing electionTicker as %v", rf.me, rf.state)
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if rf.state == Candidate {
			DPrintf("S%d executing electionTicker as %v", rf.me, rf.state)
			if rf.electionTimeoutElapse(electionTimeout) {
				DPrintf("S%d start election, election timeout elapse in Candidate", rf.me)
				rf.startElection()
			}
			DPrintf("S%d end executing electionTicker as %v", rf.me, rf.state)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTimeoutElapse(electionTimeout int) bool {
	DPrintf("S%d checking whether election timeout elapse for timeout:%v, with heartbeat:%v, added timeout:%v, time.now:%v", rf.me, electionTimeout, rf.heartbeatLastSeen, rf.heartbeatLastSeen.Add(time.Duration(electionTimeout)*time.Millisecond), time.Now())
	return rf.heartbeatLastSeen.Add(time.Duration(electionTimeout) * time.Millisecond).Before(time.Now())
}

func (rf *Raft) startElection() {
	// increment current term
	// vote for self
	// reset election timer
	// send RequestVote RPC to all other servers
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.votesReceived = 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				term := rf.currentTerm
				candidateId := rf.me
				lastLogIndex := len(rf.log) - 1
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}
				request := RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
				reply := RequestVoteReply{}
				DPrintf("S%d calling RequestVote, request:%v, from:%d", rf.me, request, server)
				rf.sendRequestVote(server, &request, &reply)
				DPrintf("S%d called RequestVote, reply:%v, from:%d", rf.me, reply, server)

				//Handle reply
				if reply.VoteGranted {
					rf.mu.Lock()
					if rf.currentTerm == term {
						rf.votesReceived += 1
						if rf.state != Leader {
							rf.tryBeLeader()
						}
					} else if rf.currentTerm < term {
						rf.state = Follower
					}

					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if rf.currentTerm < term {
						rf.state = Follower
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	DPrintf("S%d reseting election timer", rf.me)
	rf.heartbeatLastSeen = time.Now()
}
func (rf *Raft) tryBeLeader() {
	if rf.votesReceived*2 > len(rf.peers) {
		rf.state = Leader
		rf.hearbeat()
	}
}

func (rf *Raft) hearbeatTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.hearbeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) hearbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			DPrintf("S%d - sending heartbeat to S%d for term:%v", rf.me, i, rf.currentTerm)
			go func(server int) {
				term := rf.currentTerm
				leaderId := rf.me
				prevLogIndex := len(rf.log) - 1
				prevLogTerm := -1
				if prevLogIndex > -1 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				entries := []LogEntry{}
				leaderCommit := rf.commitIndex
				heartbeatArgs := AppendEntriesArgs{Term: term, LeaderId: leaderId, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: leaderCommit}
				heartbeatReply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &heartbeatArgs, &heartbeatReply)
				DPrintf("S%d - sending heartbeat to S%d for term:%v finished with reply:%v", rf.me, server, rf.currentTerm, heartbeatReply)

				// handle response
				if heartbeatReply.Success == false {
					rf.mu.Lock()
					if rf.currentTerm < heartbeatReply.Term {
						DPrintf("S%d - discover higher term", rf.me)
						rf.state = Follower
					}
					rf.mu.Unlock()
				}
			}(i)
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
	DPrintf("S%d - initializing...", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}

	rf.state = Follower
	rf.heartbeatLastSeen = time.Now()

	DPrintf("S%d - initialized with config %v", rf.me, rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.hearbeatTicker()
	return rf
}

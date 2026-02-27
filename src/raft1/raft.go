package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntries struct {
	Term    int
	Command string
}

type ServerState int

// Declare the possible values for ServerState using iota
const (
	Leader    ServerState = iota // 0
	Follower                     // 1
	Candidate                    // 2
)

var nullVotedFor = -1

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntries

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// User defined state
	status           ServerState
	isStartElection  bool
	numVotesReceived int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.status == Leader
	slog.Info("GetState: ", "Server", rf.me, "Term", rf.currentTerm, "Status", rf.status)
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	slog.Debug("Function AppendEntries", "Server", rf.me, "Term", rf.currentTerm)

	if args.Term > rf.currentTerm {
		// convert to a Follower
		slog.Debug("becoming a follower", "Server", rf.me, "Term", rf.currentTerm)
		rf.votedFor = nullVotedFor
		rf.status = Follower
		rf.currentTerm = args.Term
	}

	isSuccess := false
	if args.Term < rf.currentTerm {
		isSuccess = false
	} else {
		isSuccess = true
	}

	// heartbeat
	if len(args.Entries) == 0 {
		slog.Debug("received heartbeat", "Server", rf.me, "Term", args.Term, "Leader", args.LeaderId)
		rf.isStartElection = false
	}

	reply.Term = rf.currentTerm
	reply.Success = isSuccess
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	slog.Debug("Function RequestVote", "Server", rf.me, "Term", args.Term, "CandidateId", args.CandidateId)

	// Become a follower
	if args.Term > rf.currentTerm {
		slog.Debug("becoming a follower", "Server", rf.me, "Term", rf.currentTerm)
		rf.votedFor = nullVotedFor
		rf.status = Follower
		rf.currentTerm = args.Term
	}

	voteGranted := false
	// lenLog := len(rf.log)
	if args.Term < rf.currentTerm {
		voteGranted = false
	} else if rf.votedFor == nullVotedFor || rf.votedFor == args.CandidateId {
		// (rf.log[lenLog-1].Term < args.LastLogTerm || (rf.log[lenLog-1].Term == args.LastLogTerm && lenLog < args.LastLogIndex)) {
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.isStartElection = false
	}

	slog.Debug("Voted For", "Server", rf.me, "Term", args.Term, "CandidateId", args.CandidateId, "voteGrantedTo", voteGranted, "votedFor", rf.votedFor, "currentTerm", rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
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
//	func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//		return ok
//	}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Become a follower
	if reply.Term > rf.currentTerm {
		slog.Debug("becoming a follower", "Server", rf.me, "Term", args.Term)
		rf.votedFor = nullVotedFor
		rf.status = Follower
		rf.currentTerm = reply.Term
	} else {
		// vote counts if in the same term
		if reply.Term == args.Term && reply.VoteGranted {
			rf.numVotesReceived += 1
		}

		slog.Debug("numVotesReceived", "Server", rf.me, "Term", args.Term, "Votes", rf.numVotesReceived)
		// make into Leader if received sufficient votes
		if rf.numVotesReceived > len(rf.peers)/2 && rf.status == Candidate {
			slog.Info("Becoming a leader", "Server", rf.me, "Term", args.Term)
			rf.status = Leader
		}
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) startElection() {
	slog.Debug("Function startElection", "Server", rf.me, "Term", rf.currentTerm)
	slog.Debug("became candidate", "Server", rf.me, "Term", rf.currentTerm)
	rf.status = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.numVotesReceived = 1
	for server := range rf.peers {
		if server != rf.me {
			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(server, args, reply)
		}
	}

}

func (rf *Raft) ticker() {
	for true {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		slog.Debug("Function ticker", "Server", rf.me, "Term", rf.currentTerm)
		if rf.isStartElection {
			rf.startElection()
		}
		rf.isStartElection = true
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// convert to Follower
	if reply.Term > rf.currentTerm {
		slog.Debug("Becoming a follower", "Server", rf.me, "Term", rf.currentTerm)
		rf.votedFor = nullVotedFor
		rf.status = Follower
		rf.currentTerm = reply.Term
	}
	return ok
}

func (rf *Raft) sendHeartbeats() {
	for true {
		// Your code here (3A)
		rf.mu.Lock()
		slog.Debug("Function sendHeartbeats", "Server", rf.me, "Term", rf.currentTerm)
		if rf.status == Leader {
			for server := range rf.peers {
				if server != rf.me {
					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: []LogEntries{}, LeaderCommit: rf.commitIndex}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(server, args, reply)
				}
			}
		}
		rf.mu.Unlock()
		// send heart beats every 10 milliseconds
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = nullVotedFor
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.numVotesReceived = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeats()

	return rf
}

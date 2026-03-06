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
	Command interface{}
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
	applyCh          chan raftapi.ApplyMsg
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
	// slog.Info("GetState: ", "Server", rf.me, "Term", rf.currentTerm, "Status", rf.status)
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

func (rf *Raft) becomeAFollower(currentTerm int) {
	slog.Debug("becoming a follower", "Server", rf.me, "Term", currentTerm)
	rf.votedFor = nullVotedFor
	rf.status = Follower
	rf.currentTerm = currentTerm
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
	if args.Term >= rf.currentTerm {
		// heartbeat
		// slog.Debug("received heartbeat", "Server", rf.me, "Term", args.Term, "Leader", args.LeaderId)
		rf.isStartElection = false
	}

	// convert to a Follower
	if args.Term > rf.currentTerm {
		rf.becomeAFollower(args.Term)
	}

	var isSuccess bool
	// leader term is behind so you can't append anything
	if (args.Term < rf.currentTerm) ||
		// log doesn’t contain an entry at prevLogIndex
		(len(rf.log) <= args.PrevLogIndex || args.PrevLogIndex < 0) ||
		// whose term does not matches prevLogTerm
		(rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		isSuccess = false
	} else {
		isSuccess = true
		//  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		var lastNoConflictEntry int = -1
		for i, entry := range args.Entries {
			index := i + args.PrevLogIndex + 1
			// follower log is too short
			if index >= len(rf.log) {
				lastNoConflictEntry = i
				break
			}
			// term mismatch
			if rf.log[index].Term != entry.Term {
				lastNoConflictEntry = i
				break
			}
		}
		// Append any new entries not already in the log
		if lastNoConflictEntry != -1 {
			truncateIndex := lastNoConflictEntry + args.PrevLogIndex + 1
			rf.log = rf.log[:truncateIndex]
			rf.log = append(rf.log, args.Entries[lastNoConflictEntry:]...)
		}
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
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
		rf.becomeAFollower(args.Term)
	}

	voteGranted := false
	rfLastIndex := len(rf.log) - 1
	rfLastTerm := rf.log[rfLastIndex].Term
	if args.Term < rf.currentTerm {
		voteGranted = false
	} else if (rf.votedFor == nullVotedFor || rf.votedFor == args.CandidateId) &&
		(rfLastTerm < args.LastLogTerm || (rfLastTerm == args.LastLogTerm && rfLastIndex <= args.LastLogIndex)) {
		//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
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
		rf.becomeAFollower(reply.Term)
	} else {
		// vote counts if in the same term
		if reply.Term == args.Term && reply.VoteGranted {
			rf.numVotesReceived += 1
		}

		slog.Debug("numVotesReceived", "Server", rf.me, "Term", args.Term, "Votes", rf.numVotesReceived)
		// make into Leader if received sufficient votes
		if rf.numVotesReceived > len(rf.peers)/2 && rf.status == Candidate {
			rf.initializeNextIndexAndMatchIndex()

			// slog.Info("Becoming a leader", "Server", rf.me, "Term", args.Term)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	slog.Debug("Function Start", "Server", rf.me, "Term", rf.currentTerm, "Command", command)

	isLeader = rf.status == Leader
	term = rf.currentTerm
	index = len(rf.log)

	if isLeader {
		// slog.Info("Start", "server", rf.me, "new command", command)
		entry := LogEntries{Term: term, Command: command}
		// add to log and update nextIndex and matchIndex
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = index
		rf.matchIndex[rf.me] = index

		// start aggreement
		for server := range rf.peers {
			if server != rf.me {
				// slog.Info("StartAgreement", "server", server, "new command", entry)
				entires := []LogEntries{entry}
				args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: entires, LeaderCommit: rf.commitIndex, PrevLogIndex: index - 1, PrevLogTerm: entires[0].Term}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(server, args, reply)
			}
		}
		go rf.updateCommitindex()
	}
	return index, term, isLeader

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
			// start election
			slog.Debug("became candidate", "Server", rf.me, "Term", rf.currentTerm)
			rf.status = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.numVotesReceived = 1
			for server := range rf.peers {
				if server != rf.me {
					lastLogIndex := len(rf.log) - 1
					lastLogTerm := rf.log[lastLogIndex].Term
					args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
					reply := &RequestVoteReply{}
					go rf.sendRequestVote(server, args, reply)
				}
			}
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
		rf.becomeAFollower(reply.Term)
	}

	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower
		// slog.Info("AppendedEntires", "server", server, "log", rf.log, "args", args)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// TODO: How do ou know if the AppendEntries fails because of log inconsistency as opposed to something else
		// If AppendEntries fails because of log inconsistency decrement nextIndex and retry
		rf.nextIndex[server] -= 1
		rf.nextIndex[server] = max(rf.nextIndex[server], 1)
	}

	return ok
}

func (rf *Raft) updateCommitindex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == Leader {
		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		for N := rf.commitIndex + 1; N < len(rf.log); N++ {
			numPeersWithEntry := 0
			for server := range rf.peers {
				// slog.Info("matchIndex", "server", server, "matchIndex", rf.matchIndex[server], "N", N)
				if rf.matchIndex[server] >= N {
					numPeersWithEntry++
				}
			}
			if numPeersWithEntry > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
				// slog.Info("updateCommitindex", "server", rf.me, "numPeersWithEntry", numPeersWithEntry, "N", N)
				rf.commitIndex = N
			}
		}
	}
}

func (rf *Raft) leaderRepeat() {
	for true {
		// Your code here (3A)
		rf.mu.Lock()
		slog.Debug("Function sendHeartbeats", "Server", rf.me, "Term", rf.currentTerm)
		if rf.status == Leader {
			for server := range rf.peers {
				if server != rf.me {
					nextIndex := rf.nextIndex[server]
					prevLogIndex := nextIndex - 1
					lastLogIndex := len(rf.log) - 1
					entries := rf.log[nextIndex : lastLogIndex+1]
					// slog.Info("leaderRepeat", "lastLogIndex", lastLogIndex, "nextIndex", nextIndex, "prevLogIndex", prevLogIndex, "entries", entries)

					// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
					// slog.Info("leaderRepeat", "server", rf.me, "rf.log", rf.log, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex, "entires", entries)
					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: entries, LeaderCommit: rf.commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[prevLogIndex].Term}
					reply := &AppendEntriesReply{}
					go rf.sendAppendEntries(server, args, reply)
				}
			}
			go rf.updateCommitindex()
		}

		rf.mu.Unlock()
		// send heart beats every 10 milliseconds
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) initializeNextIndexAndMatchIndex() {
	// initialize nextIndex for all entries in nextIndex[] to leader last log index + 1 and matchIndex to be 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for server := range rf.peers {
		rf.nextIndex[server] = len(rf.log)
	}

}

func (rf *Raft) allRepeat() {
	for true {
		rf.mu.Lock()
		// All servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		// slog.Info("sending applying Msg", "rf.commitIndex", rf.commitIndex, "rf.lastApplied", rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			commitIndex := rf.lastApplied + 1
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[commitIndex].Command,
				CommandIndex: commitIndex,
			}
			rf.lastApplied++
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = nullVotedFor
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.numVotesReceived = 0
	rf.log = append(rf.log, LogEntries{Term: 0})
	rf.initializeNextIndexAndMatchIndex()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start repeatable function
	go rf.ticker()
	go rf.leaderRepeat()
	go rf.allRepeat()

	return rf
}

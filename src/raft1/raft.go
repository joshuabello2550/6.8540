package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// look into match index increasing monitonically
// unsatable leader
var NUM_MILLISECONDS_PER_HEATBEATS = 50
var NUM_MILLISECONDS_PER_LOOP = NUM_MILLISECONDS_PER_HEATBEATS / 5

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
var NULL_INT = -1

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

	snapshotIndex int // reanme to lastIncludedIndex
	snapshotTerm  int // rename to lastIncludedTerm
	snapshot      []byte
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
	slog.Debug("GetState: ", "Server", rf.me, "Term", rf.currentTerm, "Status", rf.status)
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

	slog.Debug("Persisting", "server", rf.me, "currentTerm", rf.currentTerm, "votedFor", rf.votedFor, "log", rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

func (rf *Raft) becomeAFollower(currentTerm int) {
	slog.Debug("becoming a follower", "Server", rf.me, "Term", currentTerm)
	rf.votedFor = nullVotedFor
	rf.status = Follower
	rf.currentTerm = currentTerm
	rf.persist()
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntries
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		slog.Debug("Error decoding element", "currentTerm", currentTerm, "votedFor", votedFor, "log", log)
	} else {
		slog.Debug("Restoring", "server", rf.me, "currentTerm", currentTerm, "votedFor", votedFor, "log", log)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// index - rf.snapshotIndex
func (rf *Raft) getPhysicalIndex(index int) int {
	return index - rf.snapshotIndex
}

// index + rf.snapshotIndex
func (rf *Raft) getLogicalIndex(index int) int {
	return index + rf.snapshotIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	startCutoffIndex := rf.getPhysicalIndex(index)
	slog.Info("Snapshot", "rf.me", rf.me, "index", index, "startCutoffIndex", startCutoffIndex, "len(rf.log) before", len(rf.log))
	rf.log = rf.log[startCutoffIndex:] // first index is the dummy term
	slog.Info("Snapshot", "rf.me", rf.me, "len(rf.log) after", len(rf.log))
	// create a new slice so there are no pointers in discarded log entries
	newLog := make([]LogEntries, len(rf.log))
	copy(newLog, rf.log)
	rf.log = newLog

	// update the snapshot fields
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[0].Term
	rf.snapshot = snapshot
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	slog.Debug("Function InstallSnapshot", "Server", rf.me, "Term", args.Term)
	if args.Term >= rf.currentTerm {
		// heartbeat
		// slog.Debug("received heartbeat", "Server", rf.me, "Term", args.Term, "Leader", args.LeaderId)
		rf.isStartElection = false
	}

	// Become a follower
	if args.Term > rf.currentTerm {
		rf.becomeAFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
	} else {
		if rf.lastApplied < args.LastIncludedIndex {
			applyMsgs := []raftapi.ApplyMsg{{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}}
			go rf.applyEntries(applyMsgs)
		}
		if args.LastIncludedIndex < rf.getLogicalIndex(len(rf.log)) &&
			rf.log[rf.getPhysicalIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
			//  follower received a snapshot that describes a prefix of its log
			rf.log = rf.log[rf.getPhysicalIndex(args.LastIncludedIndex):] // first index is the dummy term
			// create a new slice so there are no pointers in discarded log entries
			newLog := make([]LogEntries, len(rf.log))
			copy(newLog, rf.log)
			rf.log = newLog
		} else {
			rf.currentTerm = args.LastIncludedTerm
			rf.log = []LogEntries{{Term: rf.currentTerm}}

		}

		rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
		rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
		rf.snapshot = args.Data
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.persist()
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// Become a follower
		if reply.Term > rf.currentTerm {
			rf.becomeAFollower(reply.Term)
		} else {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
	return ok
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
	XTerm   int
	XIndex  int
	XLen    int
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

	var isSuccess bool = false
	var XTerm int = NULL_INT
	var XIndex int = NULL_INT
	var XLen int = NULL_INT
	// leader term is behind so you can't append anything
	if args.Term < rf.currentTerm {
	} else if args.PrevLogIndex >= rf.getLogicalIndex(len(rf.log)) || args.PrevLogIndex < 0 {
		// follower is too short
		XLen = rf.getLogicalIndex(len(rf.log))
	} else if rf.log[rf.getPhysicalIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// follower's term at index doesn't match
		XTerm = rf.log[rf.getPhysicalIndex(args.PrevLogIndex)].Term
		index := 0
		for rf.log[index].Term != XTerm {
			index++
		}
		XIndex = rf.getLogicalIndex(index)
	} else {
		// follower's logs contains a log at PrevLogIndex that matches PrevLogTerm
		isSuccess = true
		//  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		var lastNoConflictEntry int = -1
		for i, entry := range args.Entries {
			index := rf.getLogicalIndex(i + args.PrevLogIndex + 1)
			// follower log is too short
			if index >= rf.getLogicalIndex(len(rf.log)) {
				lastNoConflictEntry = rf.getLogicalIndex(i)
				break
			}
			// term mismatch
			if rf.log[rf.getPhysicalIndex(index)].Term != entry.Term {
				lastNoConflictEntry = rf.getLogicalIndex(i)
				break
			}
		}
		// Append any new entries not already in the log
		if lastNoConflictEntry != -1 {
			truncateIndex := rf.getPhysicalIndex(lastNoConflictEntry) + rf.getPhysicalIndex(args.PrevLogIndex) + 1
			rf.log = rf.log[:truncateIndex]
			rf.log = append(rf.log, args.Entries[rf.getPhysicalIndex(lastNoConflictEntry):]...)
			rf.persist()
		}
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLogicalIndex(len(rf.log)-1))
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = isSuccess
	reply.XTerm = XTerm
	reply.XIndex = XIndex
	reply.XLen = XLen
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
	rfLastIndex := rf.getLogicalIndex(len(rf.log) - 1)
	rfLastTerm := rf.log[len(rf.log)-1].Term
	if args.Term < rf.currentTerm {
		voteGranted = false
	} else if (rf.votedFor == nullVotedFor || rf.votedFor == args.CandidateId) &&
		(rfLastTerm < args.LastLogTerm || (rfLastTerm == args.LastLogTerm && rfLastIndex <= args.LastLogIndex)) {
		//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		voteGranted = true
		rf.votedFor = args.CandidateId
		rf.isStartElection = false
		rf.persist()
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

	if ok {
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

				slog.Info("Becoming a leader", "Server", rf.me, "Term", args.Term)
				rf.status = Leader
			}
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
	// slog.Debug("Function Start", "Server", rf.me, "Term", rf.currentTerm, "Command", command)

	isLeader = rf.status == Leader
	term = rf.currentTerm
	index = rf.getLogicalIndex(len(rf.log))

	if isLeader {
		slog.Info("Start", "server", rf.me, "new command", command, "snapshotIndex:", rf.snapshotIndex, "len(rf.log) before new command", len(rf.log))
		entry := LogEntries{Term: term, Command: command}
		// add to log and update nextIndex and matchIndex
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = index
		rf.matchIndex[rf.me] = index
		rf.persist()

		// start aggreement
		for server := range rf.peers {
			if server != rf.me {
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

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		slog.Debug("Function ticker", "Server", rf.me, "Term", rf.currentTerm)
		if rf.isStartElection && rf.status != Leader {
			// start election
			slog.Debug("became candidate", "Server", rf.me, "Term", rf.currentTerm)
			rf.status = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.numVotesReceived = 1
			rf.persist()
			slog.Debug("starting election: ", "server", rf.me, "currentTerm", rf.currentTerm)
			for server := range rf.peers {
				if server != rf.me {
					lastLogIndex := rf.getLogicalIndex(len(rf.log) - 1)
					lastLogTerm := rf.log[len(rf.log)-1].Term
					args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
					reply := &RequestVoteReply{}
					go rf.sendRequestVote(server, args, reply)
				}
			}

		}
		rf.isStartElection = true
		rf.mu.Unlock()

		// pause for a random amount of time between 15x and 30x heartbeat
		lowerBound := 15 * int64(NUM_MILLISECONDS_PER_HEATBEATS)
		ms := lowerBound + (rand.Int63() % lowerBound)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// convert to Follower
		if reply.Term > rf.currentTerm {
			rf.becomeAFollower(reply.Term)
		} else {
			if reply.Success {
				// If successful: update nextIndex and matchIndex for follower

				rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				slog.Info("successfull appendedEntires", "server", server, "nextIndex", rf.nextIndex[server], "matchIndex[", rf.matchIndex[server])
			} else {
				// If AppendEntries fails because of log inconsistency decrement nextIndex and retry
				slog.Debug("failed append entry", "leader", rf.me, "server", server, "XTerm", reply.XTerm, "XIndex", reply.XIndex, "XLen", reply.XLen)
				var nextIndex int
				// follower's log is too short
				if reply.XLen != NULL_INT {
					nextIndex = reply.XLen
				} else if rf.getIsHaveTerm(reply.Term) {
					nextIndex = reply.XIndex
				} else {
					nextIndex = rf.getLogicalIndex(rf.getLastIndexWithTerm(reply.Term) + 1)
				}
				nextIndex = max(nextIndex, 1)
				slog.Debug("nextIndex", "server", rf.me, "nextIndex", nextIndex)
				rf.nextIndex[server] = nextIndex
			}
		}
	}

	return ok
}

func (rf *Raft) getIsHaveTerm(term int) bool {
	index := 0
	for index < len(rf.log) {
		if rf.log[index].Term == term {
			return true
		}
		index++
	}
	return false
}

func (rf *Raft) getLastIndexWithTerm(term int) int {
	index := len(rf.log) - 1
	for index > 0 {
		if rf.log[index].Term == term {
			break
		}
		index--
	}
	return index
}

func (rf *Raft) updateCommitindex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == Leader {
		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		for N := rf.commitIndex + 1; N < rf.getLogicalIndex(len(rf.log)); N++ {
			numPeersWithEntry := 0
			for server := range rf.peers {
				if rf.matchIndex[server] >= N {
					numPeersWithEntry++
				}
			}
			slog.Debug("before updateCommitindex", "server", rf.me, "numPeersWithEntry", numPeersWithEntry, "rf.commitIndex", N, "rf.log[N].Term: ", rf.log[rf.getPhysicalIndex(N)].Term, "rf.currentTerm", rf.currentTerm)
			if numPeersWithEntry > len(rf.peers)/2 && rf.log[rf.getPhysicalIndex(N)].Term == rf.currentTerm {
				slog.Debug("updateCommitindex", "server", rf.me, "numPeersWithEntry", numPeersWithEntry, "rf.commitIndex", N, "len log: ", len(rf.log))
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
					// leader has the log follower wants
					if nextIndex > rf.snapshotIndex {
						prevLogIndex := nextIndex - 1
						entries := rf.log[rf.getPhysicalIndex(nextIndex):len(rf.log)]

						// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
						slog.Info("leaderRepeat sendAppendEntries", "server", server, "leader", rf.me, "nextIndex", nextIndex, "len(entries)", len(entries))
						args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: entries, LeaderCommit: rf.commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: rf.log[rf.getPhysicalIndex(prevLogIndex)].Term}
						reply := &AppendEntriesReply{}
						go rf.sendAppendEntries(server, args, reply)
					} else {
						slog.Info("leaderRepeat sendInstallSnapshot", "server", server, "leader", rf.me, "nextIndex", nextIndex, "rf.snapshotIndex", rf.snapshotIndex)

						// leader has already discarded the next log entry that it needs to send to a follower
						args := &InstallSnapshotArgs{Term: rf.currentTerm, LastIncludedIndex: rf.snapshotIndex, LastIncludedTerm: rf.snapshotTerm, Data: rf.snapshot}
						reply := &InstallSnapshotReply{}
						go rf.sendInstallSnapshot(server, args, reply)
					}

				}
			}
			go rf.updateCommitindex()
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(NUM_MILLISECONDS_PER_HEATBEATS))
	}
}

func (rf *Raft) initializeNextIndexAndMatchIndex() {
	// initialize nextIndex for all entries in nextIndex[] to leader last log index + 1 and matchIndex to be rf.snapshotIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for server := range rf.peers {

		rf.nextIndex[server] = rf.getLogicalIndex(len(rf.log))
		rf.matchIndex[server] = rf.snapshotIndex
	}

}

func (rf *Raft) allRepeat() {
	for true {
		rf.mu.Lock()
		// All servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		// create a list of all the the entires that need to be applied
		applyMsgs := make([]raftapi.ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			commitIndex := rf.lastApplied + 1
			command := rf.log[rf.getPhysicalIndex(commitIndex)].Command
			applyMsgs = append(applyMsgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: commitIndex,
			})

			rf.lastApplied++
		}

		rf.mu.Unlock()
		if len(applyMsgs) > 0 {
			rf.applyEntries(applyMsgs)
		}
		time.Sleep(time.Millisecond * time.Duration(NUM_MILLISECONDS_PER_LOOP))
	}
}

func (rf *Raft) applyEntries(applyMsgs []raftapi.ApplyMsg) {
	rf.mu.Lock()
	slog.Info("applyEntries", "server", rf.me, "len(applyMsgs)", len(applyMsgs), "rf.lastApplied", rf.lastApplied, "rf.commitIndex", rf.commitIndex)
	rf.mu.Unlock()
	// apply all the messages in applyMsgs
	for _, msg := range applyMsgs {
		rf.applyCh <- msg
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
	opts := &slog.HandlerOptions{
		Level: slog.LevelError,
	}
	var logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// values if there is no saved state
	rf.snapshotTerm = 0
	rf.snapshotIndex = 0
	rf.log = []LogEntries{{Term: 0}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex

	rf.votedFor = nullVotedFor
	rf.status = Follower
	rf.numVotesReceived = 0

	// start repeatable function
	go rf.ticker()
	go rf.leaderRepeat()
	go rf.allRepeat()
	return rf
}

func (rf *Raft) Kill() {
	close(rf.applyCh)
}

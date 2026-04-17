package rsm

import (
	"bufio"
	"log/slog"
	"os"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid" // QUESTION: Is it safe to use this?
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  string
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type ExpectedValues struct {
	expectedLogIndex int
	expectedTerm     int
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	commands           map[string]chan any
	expectedValues     map[string]ExpectedValues
	haveLostLeadership map[string]bool
	logIndexToId       map[int]string
	latestCommitIndex  int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	var logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	rsm := &RSM{
		me:                 me,
		maxraftstate:       maxraftstate,
		applyCh:            make(chan raftapi.ApplyMsg),
		sm:                 sm,
		commands:           make(map[string]chan any),
		expectedValues:     make(map[string]ExpectedValues),
		haveLostLeadership: make(map[string]bool),
		logIndexToId:       make(map[int]string),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// when a rsm server restarts, it should read the snapshot and, if the snapshot's length is greater than zero,
	// pass the snapshot to the StateMachine's Restore()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sm.Restore(snapshot)
	}

	go rsm.readerGoroutine()
	go rsm.isLeader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) saveSnapShot() {
	// save snapshot whenever size of raft is within 5 bytes
	if rsm.maxraftstate < rsm.rf.PersistBytes()-5 {
		snapshot := rsm.sm.Snapshot()
		rsm.rf.Snapshot(rsm.latestCommitIndex, snapshot)
	}
}

func (rsm *RSM) readerGoroutine() {
	for {
		applyMsg, ok := <-rsm.applyCh
		// stop the infinte loop if the raft server is shutdown
		if !ok {
			break
		}

		if applyMsg.SnapshotValid && applyMsg.CommandValid {
			panic("SnapshotValid and CommandValid should never be both true")
		}

		if applyMsg.SnapshotValid {
			// TODO: may need to pass more values to Restore
			snapshot := applyMsg.Snapshot
			rsm.sm.Restore(snapshot)

			rsm.mu.Lock()
			rsm.latestCommitIndex = applyMsg.SnapshotIndex
			if applyMsg.SnapshotIndex < rsm.latestCommitIndex {
				panic("snapshotIndex should never be less than the latestCommitIndex")
			}
			rsm.mu.Unlock()
		}

		if applyMsg.CommandValid {
			slog.Info("readerGoroutine", "received applyMsg", applyMsg)
			req := applyMsg.Command.(Op).Req
			id := applyMsg.Command.(Op).Id

			doOpResult := rsm.sm.DoOp(req)

			rsm.mu.Lock()
			expectedLogIndex := rsm.expectedValues[id].expectedLogIndex
			expectedTerm := rsm.expectedValues[id].expectedTerm
			currentTerm, _ := rsm.rf.GetState()
			// the channelId may be different that the Op id if the channelId's server lost leadership
			channelId := rsm.logIndexToId[applyMsg.CommandIndex]
			channel := rsm.commands[channelId]
			rsm.latestCommitIndex = applyMsg.CommandIndex
			rsm.mu.Unlock()

			if channel != nil {
				// is not the leader as the index of the command is in the wrong place or the term has changed
				if channelId != id || applyMsg.CommandIndex != expectedLogIndex || expectedTerm != currentTerm {
					rsm.mu.Lock()
					rsm.haveLostLeadership[channelId] = true
					rsm.mu.Unlock()
				}
				channel <- doOpResult
			}

		}

		rsm.saveSnapShot() // TODO: may be an issue where you save the snapshot before and then trie to read form it before it has been applied
	}
}

func (rsm *RSM) isLeader() {
	for {
		_, isLeader := rsm.rf.GetState()
		if !isLeader {
			// if no longer the leader close all pending channels
			rsm.mu.Lock()
			// fmt.Println("isLeader", "server: ", rsm.me, "rsm.commands: ", rsm.commands)
			for channelId, channel := range rsm.commands {
				rsm.haveLostLeadership[channelId] = true
				if channel != nil {
					channel <- ""
				}
			}
			rsm.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	writer := bufio.NewWriter(os.Stdout)
	writer.Flush()
	// slog.Info("Submit", "req", req)

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	id := uuid.New().String()
	op := Op{Id: id, Req: req}

	expectedLogIndex, expectedTerm, isLeader := rsm.rf.Start(op)
	// fmt.Println("Submit", "server: ", rsm.me, "isLeader: ", isLeader, "req: ", req, "expectedLogIndex: ", expectedLogIndex, "expectedTerm: ", expectedTerm)
	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	rsm.mu.Lock()
	channel := make(chan any)
	rsm.commands[id] = channel
	rsm.haveLostLeadership[id] = false

	rsm.logIndexToId[expectedLogIndex] = id
	rsm.expectedValues[id] = ExpectedValues{
		expectedLogIndex: expectedLogIndex,
		expectedTerm:     expectedTerm,
	}

	rsm.mu.Unlock()

	doOpResult := <-channel

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// respond to client after receiving response from raft
	delete(rsm.commands, id)
	close(channel)
	if rsm.haveLostLeadership[id] {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	} else {
		return rpc.OK, doOpResult
	}
}

func init() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	var logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

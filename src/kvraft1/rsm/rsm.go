package rsm

import (
	"log/slog"
	"os"
	"sync"

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
	Me  int
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

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	commands map[string]chan any
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
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		commands:     make(map[string]chan any),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.readerGoroutine()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) readerGoroutine() {
	for {
		applyMsg, ok := <-rsm.applyCh
		if !ok {
			break
		}

		slog.Info("readerGoroutine", "received applyMsg", applyMsg)
		req := applyMsg.Command.(Op).Req
		id := applyMsg.Command.(Op).Id
		me := applyMsg.Command.(Op).Me

		doOpResult := rsm.sm.DoOp(req)

		if rsm.me == me {
			rsm.commands[id] <- doOpResult
		}

	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	slog.Info("Submit", "req", req)

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	id := uuid.New().String()
	op := Op{Me: rsm.me, Id: id, Req: req}

	rsm.commands[id] = make(chan any)

	_, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		close(rsm.commands[id])
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	doOpResult := <-rsm.commands[id]
	close(rsm.commands[id])
	return rpc.OK, doOpResult

}

func init() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	var logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

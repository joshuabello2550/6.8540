package kvraft

import (
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	// Your definitions here.
	mu      sync.Mutex
	records map[string]*Record
}

type Record struct {
	value   string
	version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		key := args.Key
		record, ok := kv.records[key]
		if ok {
			reply.Err = rpc.OK
			reply.Value = record.value
			reply.Version = record.version
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}

		key, value, version := args.Key, args.Value, args.Version
		record, ok := kv.records[key]
		// key in record
		if ok {
			// version don't match
			if record.version != version {
				reply.Err = rpc.ErrVersion
			} else {
				reply.Err = rpc.OK
				kv.records[key].value = value
				kv.records[key].version += 1
			}
		} else {
			// add record if first entry
			if version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {

				reply.Err = rpc.OK
				newRecord := Record{value: value, version: version + 1}
				kv.records[key] = &newRecord
			}

		}
		return reply
	}
	panic(fmt.Errorf("req: %+v, should be either a put or a get", req))
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	rpcError, val := kv.rsm.Submit(*args)

	// rpcError equals rpc.ErrWrongLeader in this case
	if rpcError != rpc.OK {
		reply.Err = rpcError
		return
	}

	result := val.(rpc.GetReply)
	reply.Err = result.Err
	reply.Value = result.Value
	reply.Version = result.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	rpcError, val := kv.rsm.Submit(*args)

	// rpcError equals rpc.ErrWrongLeader in this case
	if rpcError != rpc.OK {
		reply.Err = rpcError
		return
	}

	result := val.(rpc.PutReply)
	reply.Err = result.Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, records: make(map[string]*Record)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

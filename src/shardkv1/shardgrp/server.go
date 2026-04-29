package shardgrp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu         sync.Mutex
	records    map[string]*Record
	largestNum [shardcfg.NShards]shardcfg.Tnum // initialized to be all zeros
	shards     [shardcfg.NShards]bool          // shards that this server owns
}

type Record struct {
	Value   string
	Version rpc.Tversion
}

type Snapshot struct {
	Records map[string]*Record
	Index   int
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		key := args.Key
		// reject Gets for keys in NOT shard it owns
		keyShard := shardcfg.Key2Shard(key)
		if ok := kv.shards[keyShard]; !ok {
			reply.Err = rpc.ErrWrongGroup
		} else {
			record, ok := kv.records[key]
			if ok {
				reply.Err = rpc.OK
				reply.Value = record.Value
				reply.Version = record.Version
			} else {
				reply.Err = rpc.ErrNoKey
			}
		}
		return reply

	case rpc.PutArgs:
		reply := rpc.PutReply{}

		key, value, version := args.Key, args.Value, args.Version

		// reject Puts for keys in NOT shard it owns
		keyShard := shardcfg.Key2Shard(key)
		if ok := kv.shards[keyShard]; !ok {
			reply.Err = rpc.ErrWrongGroup
		} else {
			record, ok := kv.records[key]
			// key in record
			if ok {
				// version don't match
				if record.Version != version {
					reply.Err = rpc.ErrVersion
				} else {
					reply.Err = rpc.OK
					kv.records[key].Value = value
					kv.records[key].Version += 1
				}
			} else {
				// add record if first entry
				if version != 0 {
					reply.Err = rpc.ErrNoKey
				} else {
					reply.Err = rpc.OK
					newRecord := Record{Value: value, Version: version + 1}
					kv.records[key] = &newRecord
				}
			}
		}
		return reply

	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}

		// get the data of the frozen shard and pass to reply
		data := make(map[string]*Record)
		for key, value := range kv.records {
			keyShard := shardcfg.Key2Shard(key)

			if args.Shard == keyShard {
				data[key] = value
			}
		}

		state, err := json.Marshal(data)
		if err != nil {
			panic("Marshal error: " + err.Error())
		}

		// set this shard to inactive
		kv.shards[args.Shard] = false

		reply.State = state
		reply.Err = rpc.OK
		reply.Num = args.Num
		return reply

	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}

		// add the data from state into records
		data := make(map[string]*Record)
		err := json.Unmarshal(args.State, &data)
		if err != nil {
			panic("UnMarshall error: " + err.Error())
		}

		for key, value := range data {
			kv.records[key] = value
		}

		// set this shard to active
		kv.shards[args.Shard] = true

		reply.Err = rpc.OK
		return reply

	case shardrpc.DeleteShardArgs:
		reply := shardrpc.DeleteShardReply{}

		// delete data that map to this shard from the records
		for key := range kv.records {
			keyShard := shardcfg.Key2Shard(key)

			if args.Shard == keyShard {
				delete(kv.records, key)
			}
		}

		reply.Err = rpc.OK
		return reply
	}
	panic(fmt.Errorf("req: %+v, should be either a put or a get", req))
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := Snapshot{Records: kv.records}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	bytes := w.Bytes()
	return bytes
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot Snapshot
	if d.Decode(&snapshot) != nil {
		fmt.Println("Error decoding element", "records", snapshot)
	} else {
		kv.records = snapshot.Records
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	rpcError, val := kv.rsm.Submit(*args)

	// must be either
	if rpcError != rpc.OK && rpcError != rpc.ErrWrongLeader {
		panic("Not valid: " + rpcError)
	}

	if rpcError == rpc.ErrWrongLeader {
		reply.Err = rpcError
		return
	}

	result := val.(rpc.GetReply)
	reply.Err = result.Err
	reply.Value = result.Value
	reply.Version = result.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	rpcError, val := kv.rsm.Submit(*args)

	// must be either
	if rpcError != rpc.OK && rpcError != rpc.ErrWrongLeader {
		panic("Not valid: " + rpcError)
	}

	if rpcError == rpc.ErrWrongLeader {
		reply.Err = rpcError
		return
	}

	result := val.(rpc.PutReply)
	reply.Err = result.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here

	// reject old RPCs
	largestNum := kv.largestNum[args.Shard]
	if args.Num < largestNum {
		reply.Err = rpc.ErrVersion
		return
	} else {
		kv.largestNum[args.Shard] = args.Num
	}

	rpcError, val := kv.rsm.Submit(*args)

	// must be either
	if rpcError != rpc.OK && rpcError != rpc.ErrWrongLeader {
		panic("Not valid: " + rpcError)
	}

	if rpcError == rpc.ErrWrongLeader {
		reply.Err = rpcError
		return
	}

	result := val.(shardrpc.FreezeShardReply)
	reply.State = result.State
	reply.Num = result.Num
	reply.Err = result.Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here

	// reject old RPCs
	largestNum := kv.largestNum[args.Shard]
	if args.Num < largestNum {
		reply.Err = rpc.ErrVersion
		return
	} else {
		kv.largestNum[args.Shard] = args.Num
	}

	rpcError, val := kv.rsm.Submit(*args)

	// must be either
	if rpcError != rpc.OK && rpcError != rpc.ErrWrongLeader {
		panic("Not valid: " + rpcError)
	}

	if rpcError == rpc.ErrWrongLeader {
		reply.Err = rpcError
		return
	}

	result := val.(shardrpc.InstallShardReply)
	reply.Err = result.Err

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here

	// reject old RPCs
	largestNum := kv.largestNum[args.Shard]
	if args.Num < largestNum {
		reply.Err = rpc.ErrVersion
		return
	} else {
		kv.largestNum[args.Shard] = args.Num
	}

	rpcError, val := kv.rsm.Submit(*args)

	// must be either
	if rpcError != rpc.OK && rpcError != rpc.ErrWrongLeader {
		panic("Not valid: " + rpcError)
	}

	if rpcError == rpc.ErrWrongLeader {
		reply.Err = rpcError
		return
	}

	result := val.(shardrpc.DeleteShardReply)
	reply.Err = result.Err
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, records: make(map[string]*Record)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	// if this server is part ofthe inialize group initialzie it to own all shards
	if gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.shards[i] = true
		}
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}

package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	for {
		// iterate through all the servers to determine the server
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.Call(ck.servers[i], "KVServer.Get", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.leader = i
				// should never happen
				if reply.Err == rpc.ErrMaybe || reply.Err == rpc.ErrVersion {
					panic("Error cannot happen: " + reply.Err)
				}

				return reply.Value, reply.Version, reply.Err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	isfirst := true
	for {
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.Call(ck.servers[i], "KVServer.Put", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.leader = i

				if reply.Err == rpc.ErrVersion && !isfirst {
					return rpc.ErrMaybe
				} else {
					return reply.Err
				}
			}
			isfirst = false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := shardrpc.FreezeShardReply{}
			ok := ck.Call(ck.servers[i], "KVServer.FreezeShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.leader = i

				// should never happen
				// TODO: Am I accurate in saying that ErrorWrongGorup should never happen in freeze, install, delete
				if reply.Err == rpc.ErrMaybe || reply.Err == rpc.ErrWrongGroup {
					panic("Error cannot happen: " + reply.Err)
				}

				return reply.State, reply.Err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := shardrpc.InstallShardReply{}
			ok := ck.Call(ck.servers[i], "KVServer.InstallShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.leader = i

				// should never happen
				if reply.Err == rpc.ErrMaybe || reply.Err == rpc.ErrWrongGroup {
					panic("Error cannot happen: " + reply.Err)
				}

				return reply.Err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := shardrpc.DeleteShardReply{}
			ok := ck.Call(ck.servers[i], "KVServer.DeleteShard", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				ck.leader = i

				// should never happen
				if reply.Err == rpc.ErrMaybe || reply.Err == rpc.ErrWrongGroup {
					panic("Error cannot happen: " + reply.Err)
				}

				return reply.Err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

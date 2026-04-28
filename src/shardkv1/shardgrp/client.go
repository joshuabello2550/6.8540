package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	isfirst bool
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
				if reply.Err == rpc.ErrNoKey || reply.Err == rpc.OK {
					ck.leader = i
					return reply.Value, reply.Version, reply.Err
				}
			}
		}
		time.Sleep(0 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	for {
		for idx := range len(ck.servers) {
			// Trick to always start from leader
			i := (ck.leader + idx) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.Call(ck.servers[i], "KVServer.Put", &args, &reply)
			if ok && reply.Err != rpc.ErrWrongLeader {
				if reply.Err == rpc.ErrVersion && !ck.isfirst {
					ck.isfirst = true
					return rpc.ErrMaybe
				} else {
					ck.isfirst = true
					return reply.Err
				}
			}
			ck.isfirst = false
		}
		time.Sleep(0 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

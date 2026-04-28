package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	isfirst bool
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	for {
		// get start responsible for the key and then look up servers for that shard
		shard := shardcfg.Key2Shard(key)
		currentConfig := ck.sck.Query()
		gid, servers, gidServersOk := currentConfig.GidServers(shard)

		if gidServersOk {
			// make the clerk for the group if it does not exist
			if _, shardgrpClerkOK := ck.rcks[gid]; !shardgrpClerkOK {
				ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
			}

			shardgrpClerk := ck.rcks[gid]
			value, version, ok := shardgrpClerk.Get(key)

			// The shardgrp only ever returns when it is OK or NoKey
			return value, version, ok
		}

		time.Sleep(0 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	for {
		// get start responsible for the key and then look up servers for that shard
		shard := shardcfg.Key2Shard(key)
		currentConfig := ck.sck.Query()
		gid, servers, gidServersOk := currentConfig.GidServers(shard)

		if gidServersOk {
			// make the clerk for the group if it does not exist
			if _, shardgrpClerkOK := ck.rcks[gid]; !shardgrpClerkOK {
				ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
			}

			shardgrpClerk := ck.rcks[gid]
			ok := shardgrpClerk.Put(key, value, version)

			if ok == rpc.ErrMaybe || ok == rpc.OK {
				return ok
			}
		}

		time.Sleep(0 * time.Millisecond)
	}
}

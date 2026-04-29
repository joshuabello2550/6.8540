package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	currentConfigKey string
	version          rpc.Tversion // TODO: May have an issue where this si not being updated
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.currentConfigKey = "Config"
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	value := cfg.String()
	sck.IKVClerk.Put(sck.currentConfigKey, value, sck.version)
	sck.version += 1
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	currentConfig := sck.Query()

	for shard, prevGroupId := range currentConfig.Shards {
		prevServers := currentConfig.Groups[prevGroupId]

		newGroupId := new.Shards[shard]
		newServers := new.Groups[newGroupId]

		if prevGroupId != newGroupId {
			shard := shardcfg.Tshid(shard)
			prevShardGrpClerk := shardgrp.MakeClerk(sck.clnt, prevServers)
			newShardGrpClerk := shardgrp.MakeClerk(sck.clnt, newServers)

			// "freeze" the shard at the source shardgrp
			state, _ := prevShardGrpClerk.FreezeShard(shard, currentConfig.Num)

			// copy (install) the shard to the destination shardgrp
			newShardGrpClerk.InstallShard(shard, state, new.Num)

			// then delete the frozen shard
			prevShardGrpClerk.DeleteShard(shard, currentConfig.Num)

			// post a new configuration so that clients can find the moved shard
			value := new.String()
			sck.IKVClerk.Put(sck.currentConfigKey, value, sck.version)
			sck.version += 1
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, _ := sck.IKVClerk.Get(sck.currentConfigKey)
	currentConfig := shardcfg.FromString(value)
	return currentConfig
}

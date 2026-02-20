package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	records map[string]*Record
}

type Record struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{records: make(map[string]*Record)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	record, ok := kv.records[key]
	if ok {
		reply.Err = rpc.OK
		reply.Value = record.value
		reply.Version = record.version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

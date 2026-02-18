package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockName string
	ckUUID   string
}

const NoClient = "NONE"

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	ckUUID := kvtest.RandValue(8)
	lk := &Lock{ck: ck, ckUUID: ckUUID, lockName: lockname}

	// add the lock to the server if it doesn't exist
	_, _, err := lk.ck.Get(lockname)
	if err == rpc.ErrNoKey {
		lk.ck.Put(lockname, NoClient, rpc.Tversion(0))
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, _ := lk.ck.Get(lk.lockName)
		if value == NoClient {
			err := lk.ck.Put(lk.lockName, lk.ckUUID, version)
			if err == rpc.OK {
				break
			}
		}
	}
}

func (lk *Lock) Release() {
	value, version, _ := lk.ck.Get(lk.lockName)
	if value == lk.ckUUID {
		lk.ck.Put(lk.lockName, NoClient, version)
	}
}

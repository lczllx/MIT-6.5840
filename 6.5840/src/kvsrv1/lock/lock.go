package lock

import (
	"time"

	kvsrv "6.5840/kvsrv1"
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
	LockName string
	myID     string //客户端唯一标识
	Version  rpc.Tversion
	Err      rpc.Err
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {

	kvsrv.DPrintf("in MakeLock")
	myID := kvtest.RandValue(8) //生成客户端唯一标识
	lk := &Lock{
		ck:       ck,
		LockName: lockname,
		myID:     myID,
		Version:  0,
		Err:      "",
	}
	// You may add code here

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	kvsrv.DPrintf("in Acquire")
	for {
		// 1. 获取当前锁状态
		val, version, err := lk.ck.Get(lk.LockName)

		if err == rpc.ErrNoKey {
			//锁没有被申请
			putErr := lk.ck.Put(lk.LockName, lk.myID, lk.Version)
			if putErr == rpc.OK {
				// 成功获取锁，记录版本（服务器会设为1）
				lk.Version = 1
				return
			}
			// 如果 Put 返回 ErrMaybe 或 ErrVersion，说明可能被其他人抢先，重试
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err == rpc.OK {
			if val == "" {
				// 锁空闲，尝试获取（使用当前版本 ver）
				putErr := lk.ck.Put(lk.LockName, lk.myID, version)
				if putErr == rpc.OK {
					// 成功，新版本为 ver+1
					lk.Version = version + 1
					return
				}
				// 如果 Put 返回 ErrVersion，说明版本已被修改（别人抢先），重试
				// 如果返回 ErrMaybe，也重试（不确定是否成功，但大概率失败）
			} else if val == lk.myID {
				// 自己已经持有锁（可能是之前 Acquire 成功但返回前网络问题）
				// 直接返回，版本以当前为准
				lk.Version = version
				return
			}
			// 否则锁被他人持有，等待重试
		}

		// 其他错误（如 ErrMaybe、ErrVersion 等）都重试
		time.Sleep(5 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	kvsrv.DPrintf("in Release")
	for {
		val, version, err := lk.ck.Get(lk.LockName)
		if err == rpc.ErrNoKey {
			// 锁已不存在（理论上不会发生，但安全起见返回）
			return
		}

		if err == rpc.OK {
			if val == lk.myID {
				// 是自己持有的锁，尝试释放（置空值，使用当前版本）
				putErr := lk.ck.Put(lk.LockName, "", version)
				if putErr == rpc.OK {
					// 释放成功
					lk.Version = 0
					return
				}
				// 如果 Put 返回 ErrVersion，说明版本已被修改（可能被他人强制释放？），重试
				// 如果返回 ErrMaybe，也重试
			} else {
				// 不是自己持有的锁，可能已经释放或别人持有，直接返回
				// 但为了安全，可以重试或返回（根据规范，调用 Release 的应该是持有者）
				return
			}
		}

		// 其他错误重试
		time.Sleep(5 * time.Millisecond)
	}
}

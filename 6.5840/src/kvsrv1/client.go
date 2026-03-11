package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// 客户端描述结构体
type Clerk struct {
	clnt   *tester.Clnt //客户端句柄
	server string       //服务器名称
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	DPrintf("in MakeClerk")
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	DPrintf("in client Get")
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	if ok {
		return reply.Value, reply.Version, reply.Err
	}
	DPrintf("in get retry")
	for {
		ok = ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			return reply.Value, reply.Version, reply.Err //第一次测试时没有加，导致测试不通过
		}
		time.Sleep(5 * time.Millisecond)
	}
	//return "", 0, rpc.ErrNoKey
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	DPrintf("in client Put")
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	isfirst := true

	//不能限制其重试的次数
	// retries := 0
	// maxRetries := 3  // 最大重试次数
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

		if !ok { // RPC 失败（网络问题），标记已发送，继续重试
			DPrintf("Put RPC failed, retrying...")
			isfirst = false
			// retries++
			// if retries >= maxRetries {
			// 	return rpc.ErrNoKey
			// }
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			return reply.Err
		}

		// 第一次“有效回复”且为 ErrVersion → 肯定没执行；曾发生过 ok==false 再收到 ErrVersion → 可能已执行过
		if reply.Err == rpc.ErrVersion {
			if isfirst {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		time.Sleep(5 * time.Millisecond)
	}
	//return rpc.ErrNoKey
}

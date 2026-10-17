package kvraft

import (
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

var clientIdCounter int64

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	seqId    int
	clientId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = 0
	ck.seqId = 0
	ck.clientId = int(atomic.AddInt64(&clientIdCounter, 1))
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	ck.seqId++
	args := rpc.GetArgs{key, int64(ck.clientId), int64(ck.seqId)}
	serverId := ck.leader
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrNoKey {
				ck.leader = serverId
				return reply.Value, reply.Version, reply.Err
			} else if reply.Err == rpc.OK {
				ck.leader = serverId
				return reply.Value, reply.Version, reply.Err
			} else if reply.Err == rpc.ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}

		}
		serverId = (serverId + 1) % len(ck.servers)

	}
	return "", 0, ""
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	ck.seqId++
	args := rpc.PutArgs{key, value, version, int64(ck.clientId), int64(ck.seqId)}
	serverId := ck.leader
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leader = serverId
				return reply.Err
			} else if reply.Err == rpc.ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			} else if reply.Err == rpc.ErrVersion {
				ck.leader = serverId
				return rpc.ErrMaybe
			}
		}
		serverId = (serverId + 1) % len(ck.servers)

	}
	return ""
}

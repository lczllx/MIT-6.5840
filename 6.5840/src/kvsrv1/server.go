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

// 值和版本 结构
type kvEntry struct {
	value   string
	version rpc.Tversion
}

// 服务端数据结构
type KVServer struct {
	mu   sync.Mutex
	data map[string]*kvEntry //key -[value,version]
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]*kvEntry),
	}
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
	if entry, ok := kv.data[key]; ok {
		DPrintf("get find it")
		reply.Value = entry.value
		reply.Version = entry.version
		reply.Err = rpc.OK
		return
	} else {
		DPrintf("get no find it")
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
	key := args.Key
	if entry, ok := kv.data[key]; ok { //key存在
		DPrintf("put find it")
		if args.Version == entry.version {
			DPrintf("args.Version==entry.version")
			entry.value = args.Value
			entry.version++
			reply.Err = rpc.OK
		} else {
			DPrintf("args.Version!=entry.version")
			reply.Err = rpc.ErrVersion
		}
		return
	} else { //key不存在
		DPrintf("put no find it")
		if args.Version == 0 {
			DPrintf("args.Version==0")
			kv.data[key] = &kvEntry{
				value:   args.Value,
				version: 1,
			}
			reply.Err = rpc.OK //第一次测试时没有加，导致测试不通过
		} else {
			DPrintf("args.Version!=0")
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

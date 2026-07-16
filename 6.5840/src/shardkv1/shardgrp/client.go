package shardgrp

import (
	"crypto/rand"
	"encoding/binary"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	ClntId int64 // 客户端唯一id ，MakeClerk 时随机生成
	SeqNum int64 // 客户的请求编号， 每次 Get/Put 前 +1
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers, ClntId: newClientID(), SeqNum: 0}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	ck.SeqNum++
	args := rpc.GetArgs{Key: key, ClntId: ck.ClntId, SeqNum: ck.SeqNum}
	reply := rpc.GetReply{}

	// 从上次成功的 leader 开始试
	for i := 0; i < len(ck.servers); i++ {
		srv := ck.servers[(ck.leader+i)%len(ck.servers)]
		ok := ck.Call(srv, "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leader = (ck.leader + i) % len(ck.servers)
			return reply.Value, reply.Version, reply.Err
		}
	}
	return "", 0, rpc.ErrWrongLeader

}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	ck.SeqNum++
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClntId: ck.ClntId, SeqNum: ck.SeqNum}
	reply := rpc.PutReply{}

	// 从上次成功的 leader 开始试
	for i := 0; i < len(ck.servers); i++ {
		srv := ck.servers[(ck.leader+i)%len(ck.servers)]
		ok := ck.Call(srv, "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leader = (ck.leader + i) % len(ck.servers)
			return reply.Err
		}
	}
	return ""
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

func newClientID() int64 {
	var buf [8]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic("failed to generate client ID")
	}
	// 转为有符号 int64，但去重时只关心值相等，用 uint64 更好
	return int64(binary.BigEndian.Uint64(buf[:]))
}

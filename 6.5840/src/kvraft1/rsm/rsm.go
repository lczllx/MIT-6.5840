package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req    any   //请求类型
	ClntId int64 //clerk 的编号
	SeqNum int64 //这个clerk的请求编号
}

type result struct {
	err rpc.Err
	val any
}
type dupResult struct {
	seqNum int64
	result any // DoOp 的返回值，重试时直接返回
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // 快照前最大日志数/snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	lastApplied map[int64]*dupResult //clntId →  seqNum,result
	waitTable   map[int]chan result  //index->等待chanel
	killed      atomic.Bool
	opTimeout   time.Duration
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		lastApplied:  make(map[int64]*dupResult),
		waitTable:    make(map[int]chan result),
		killed:       atomic.Bool{},
		opTimeout:    2 * time.Second,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any, clntId int64, seqNum int64) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	// your code here
	rsm.mu.Lock()

	op := Op{Req: req, ClntId: clntId, SeqNum: seqNum}
	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	res_ch := make(chan result)
	rsm.waitTable[index] = res_ch
	rsm.mu.Unlock()
	select {
	case ret := <-res_ch:
		return ret.err, ret.val
	case <-time.After(rsm.opTimeout):
		rsm.mu.Lock()
		delete(rsm.waitTable, index) // 自己删
		rsm.mu.Unlock()
	}
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

// applier goroutine — 一个单独的 goroutine，在 MakeRSM 时启动
func (rsm *RSM) applier() {
	for msg := range rsm.applyCh { // 不断从 applyCh 读
		if msg.CommandValid {
			// msg.CommandIndex = 42, msg.Command = Op{Req: PutArgs{x, 1}, ...}
			rsm.mu.Lock()
			op := msg.Command.(Op)
			dup, ok := rsm.lastApplied[op.ClntId]
			var res any
			if ok && op.SeqNum <= dup.seqNum { //旧请求
				res = dup.result
			} else {
				res = rsm.sm.DoOp(op) // 实际执行 Put
				rsm.lastApplied[op.ClntId] = &dupResult{seqNum: op.SeqNum, result: res}
			}
			ch, ok := rsm.waitTable[msg.CommandIndex] // 找到 index  的等待 channel
			rsm.mu.Unlock()
			if ok {
				ch <- result{err: rpc.OK, val: res}
			} // 往里面写结果
		}
	}
}

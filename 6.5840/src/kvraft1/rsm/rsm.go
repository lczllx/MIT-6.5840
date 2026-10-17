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
	Req    any   //上层请求
	ClntId int64 //clerk 的编号
	SeqNum int64 //这个clerk的请求编号
	Id     int64 //全局唯一id
}

type result struct {
	err rpc.Err
	val any
}

type dupResult struct {
	seqNum int64
	result any // DoOp 的返回值，重试时直接返回
}

type waiter struct {
	id int64
	ch chan result // 带缓冲，cap=1
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
	waitTable   map[int]*waiter      //index->等待者
	killed      atomic.Bool
	opTimeout   time.Duration
	nextId      int64 // 原子递增
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
		waitTable:    make(map[int]*waiter),
		killed:       atomic.Bool{},
		opTimeout:    2 * time.Second,
		nextId:       0,
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
	id := atomic.AddInt64(&rsm.nextId, 1)
	op := Op{Req: req, ClntId: clntId, SeqNum: seqNum, Id: id}
	rsm.mu.Lock()

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	res_ch := make(chan result, 1)

	//覆盖 index 前没通知旧 waiter
	if old, ok := rsm.waitTable[index]; ok {
		old.ch <- result{rpc.ErrWrongLeader, nil}
		delete(rsm.waitTable, index)
	}
	rsm.waitTable[index] = &waiter{id, res_ch}
	rsm.mu.Unlock()

	ticker := time.NewTicker(30 * time.Millisecond)
	deadline := time.NewTimer(rsm.opTimeout)
	defer ticker.Stop()
	defer deadline.Stop()
	for {
		select {
		case ret := <-res_ch:
			return ret.err, ret.val
		case <-ticker.C:
			// 领导权/任期变了：日志可能已被截断，该 index 永远不会 apply
			curTerm, stillLeader := rsm.rf.GetState()
			if curTerm != term || !stillLeader {
				rsm.mu.Lock()
				rsm.cleanup(index, id)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-deadline.C:
			rsm.mu.Lock()
			rsm.cleanup(index, id)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}

// applier goroutine — 一个单独的 goroutine，在 MakeRSM 时启动
func (rsm *RSM) applier() {
	for msg := range rsm.applyCh { // 不断从 applyCh 读
		if msg.CommandValid {
			// msg.CommandIndex = 42, msg.Command = Op{Req: PutArgs{x, 1}, ...}
			op, ok := msg.Command.(Op)
			//dup, ok := rsm.lastApplied[op.ClntId]

			if !ok {
				continue
			}
			res := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			w, ok := rsm.waitTable[msg.CommandIndex] // 找到 index  的等待 channel

			if ok {
				delete(rsm.waitTable, msg.CommandIndex)
				rsm.mu.Unlock()

				if w.id == op.Id {
					w.ch <- result{rpc.OK, res}
				} else {
					// 这个 index 已被别的命令覆盖：返回别人的结果会破坏正确性
					w.ch <- result{rpc.ErrWrongLeader, nil}
				}
			} else {
				rsm.mu.Unlock()

			}
		} else {
			continue
		}
	}
	rsm.mu.Lock()
	rsm.failAllWaiters(rpc.ErrWrongLeader)
	rsm.mu.Unlock()
}

func (rsm *RSM) cleanup(index int, id int64) {
	if w, ok := rsm.waitTable[index]; ok && w.id == id {
		delete(rsm.waitTable, index)
	}
}
func (rsm *RSM) failAllWaiters(err rpc.Err) {
	for _, w := range rsm.waitTable {
		w.ch <- result{err, nil} // ch cap=1，不会阻塞
	}
	rsm.waitTable = make(map[int]*waiter)

}

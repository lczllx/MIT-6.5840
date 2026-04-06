package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// 判断候选人日志是否和我的一样新 3D后修改
func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	myLastIndex := rf.getLastIndex()
	myLastTerm := rf.getTermByIndex(myLastIndex)

	if args.LastLogTerm > myLastTerm {
		return true
	}
	if args.LastLogTerm < myLastTerm {
		return false
	}
	// 任期相同，比较索引
	return args.LastLogIndex >= myLastIndex
}

// 随机选举超时时间生成 150-300ms（论文推荐）
func (rf *Raft) randElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// 日志提交应用-leader专属（matchIndex/commitIndex 均为逻辑索引）
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}
	for i := rf.getLastIndex(); i > rf.commitIndex; i-- {
		if rf.getTermByIndex(i) == rf.currentTerm {
			cnt := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2+1 {
				rf.commitIndex = i
				rf.applyCond.Broadcast()
				break
			}
		} else if rf.getTermByIndex(i) < rf.currentTerm {
			break
		}
	}
}

// 获取全局逻辑索引的日志
func (rf *Raft) getLog(logicIndex int) LogEntry {
	idx := logicIndex - rf.LastIncludedIndex //物理下标
	return rf.logs[idx]
}

// 获取日志没有被压缩的总长度
func (rf *Raft) getLogLen() int {
	return len(rf.logs) - 1 + rf.LastIncludedIndex
}

// 获取逻辑上的总长度
func (rf *Raft) getLastIndex() int {
	// 逻辑总长度 = 数组长度 - 1 + 快照截断掉的长度
	return len(rf.logs) - 1 + rf.LastIncludedIndex
}

// 获取物理下标
func (rf *Raft) getPhysicIdx(logicIndex int) int {
	return logicIndex - rf.LastIncludedIndex
}

// 获取特定索引的term
func (rf *Raft) getTermByIndex(logicIndex int) int {
	// 如果查的是快照最后一位
	if logicIndex == rf.LastIncludedIndex {
		return rf.LastIncludedTerm
	}
	// 转换物理下标
	return rf.logs[logicIndex-rf.LastIncludedIndex].Term
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 处理回复（任期更新、调整 nextIndex 等）
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return ok
		}
		// 2. 状态检查
		if rf.state != Leader || rf.term != args.Term {
			return ok
		}

		// 3B 以后你会在这里处理日志同步的 reply.Success 为 false 的情况
	}
	return ok
}

// 判断候选人日志是否和我的一样新
func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	myLastIndex := len(rf.logs) - 1
	myLastTerm := rf.logs[myLastIndex].Term

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

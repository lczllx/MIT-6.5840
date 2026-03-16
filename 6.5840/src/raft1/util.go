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
		//如果追加日志成功
		if reply.Success {
			newMathIdx := args.PrevLogIndex + len(args.Entries)
			if newMathIdx > rf.matchIndex[server] {
				rf.matchIndex[server] = newMathIdx
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			//更新提交的日志
			rf.updateCommitIndex()
		} else {
			//如果失败，根据reply.ConflictIndex实现快速跳转
			if reply.ConflictTerm == -1 {
				//日志过短
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				//存在冲突任期
				flag := false //是否存在冲突任期
				//这里不能用len(rf.logs)-1，得用历史的
				for i := args.PrevLogIndex; i > 0; i-- {
					if rf.logs[i].Term == reply.ConflictTerm {
						flag = true
						rf.nextIndex[server] = i + 1
						break
					}
				}

				if !flag {
					//如果找不到，就需要从冲突下标开始重新查找
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}

		}
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

// 日志提交应用-leader专属
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}
	//将没有应用到状态机的日志
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {

		//只能提交当前任期的日志
		if rf.logs[i].Term == rf.term {
			cnt := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i { //如果已经同步到了i
					cnt++
				}
			}
			//大多数节点应用了
			if cnt >= len(rf.peers)/2+1 {
				rf.commitIndex = i
				rf.applyCond.Broadcast() //唤醒applier
				break                    // 找到了最大的 N，后续更小的不用找了
			}
		} else if rf.logs[i].Term < rf.term {
			// 如果日志任期已经小于当前任期，根据 Raft 属性，
			// 之后更早的日志也不可能满足 "当前任期" 且 "多数派同步" 了
			break
		}

	}
}

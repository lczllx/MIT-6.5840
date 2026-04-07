package raft

import (
	"time"

	"6.5840/raftapi"
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// -------------------------------rpc请求参数和回复参数--------------------------------
// example RequestVote RPC arguments structure.
// field names must start with capital letters!

type RequestVoteArgs struct { //投票请求参数
	// Your data here (3A, 3B).
	Term         int //候选人的任期
	CandidateId  int //候选人的ID
	LastLogIndex int //候选人的最后一个日志条目的索引
	LastLogTerm  int //候选人的最后一个日志条目的任期
}
type RequestVoteReply struct { //投票回复参数
	// Your data here (3A).
	Term        int  //leader的当前任期，给候选人自行更新
	VoteGranted bool //是否获得投票

}

// 追加条目RPC请求参数
type AppendEntriesArgs struct {
	// Your data here (3B).
	Term         int        //leader的任期
	LeaderId     int        //leader的ID
	PrevLogIndex int        //紧接着新条目之前的最后一个条目的索引
	PrevLogTerm  int        //紧接着新条目之前的最后一个条目的任期
	Entries      []LogEntry //需要追加的新条目
	LeaderCommit int        //leader的CommitIndex
}

// 追加条目RPC回复参数
type AppendEntriesReply struct {
	// Your data here (3B).
	Term    int  //用于leader更新自己当前的任期
	Success bool //如果 follower 包含匹配的 prevLogIndex 和 prevLogTerm 条目，则为 true

	//快速回退字段 3B
	ConflictTerm  int //冲突任期
	ConflictIndex int //冲突位置
}

// ---------------------------------snapshot--------------------------------------
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
type InstallSnapshotArgs struct {
	Term              int    //leader的任期
	LeaderId          int    //leader的id
	LastIncludedIndex int    //最后一个被快照取代的日志条目的索引
	LastIncludedTerm  int    //LastIncludedIndex对应的任期
	Data              []byte //快照内容

	Done   bool //是否是最后一个快照块
	Offset int  //数据块在快照文件中位置的字节偏移量
}
type InstallSnapshotReply struct {
	Term int //当前任期，供leader的自我更新
}

// example RequestVote RPC handler.
// 请求投票接口
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] RequestVote from candidate=%d argsTerm=%d myTerm=%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		DPrintf("[%d] RequestVote reject: argsTerm %d < myTerm %d", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term >= rf.currentTerm {
		//在任期相等时不应该更新任期并重置投票
		//在同一任期内，如果有多个 Candidate，
		// 它们会因为收到对方的投票请求而互相降级，
		// 导致谁都选不上，甚至出现逻辑死循环
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1 //重置投票
			rf.state = Follower
			rf.persist()
		}
	}
	//检查候选人日志是否和我的一样新
	isuptodate := rf.isLogUpToDate(args)
	//没有投票或者已经投票给这个候选人并且候选人日志是否和我的一样新时
	//投票给他，重置选举计时器
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isuptodate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.lastHeartBeatTime = time.Now()
		rf.state = Follower

		DPrintf("[%d] RequestVote grant to %d term=%d", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
		DPrintf("[%d] RequestVote reject: votedFor=%d or !isUpToDate", rf.me, rf.votedFor)
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 追加条目rpc，也用来发送心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] AppendEntries from L%d term=%d, my term=%d state=%d", rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state)
	reply.Success = false
	reply.Term = rf.currentTerm

	// 发现任期比自己小
	if args.Term < rf.currentTerm {
		DPrintf("[%d] AppendEntries: 拒绝 term 更小 leader=%d argsTerm=%d myTerm=%d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// 发现更高任期或来自合法 Leader 的心跳
	rf.lastHeartBeatTime = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		DPrintf("[%d] step down to Follower due to AE term=%d", rf.me, rf.currentTerm)
	}
	rf.state = Follower

	//一致性检查
	//日志长度不够 Follower 若还没有 PrevLogIndex 这一条，才拒绝
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLastIndex() + 1 //告诉leader从我的末尾开始尝试
		return
	}

	//一致性检查：PrevLogIndex 处的 Term 是否匹配
	// 特别注意：如果 PrevLogIndex 恰好在快照边界，要用 LastIncludedTerm
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.LastIncludedIndex + 1
		return
	}

	//PrevLogIndex处任期不匹配
	if rf.getTermByIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.getTermByIndex(args.PrevLogIndex) //更新发生冲突的任期
		idx := args.PrevLogIndex
		//从最新的日志位置开始向前找，找到冲突任期的下标
		//告诉leader，这个下标是冲突任期的下标，下一步继续找冲突位置，若没有则进行同步
		for idx > rf.LastIncludedIndex && rf.getTermByIndex(idx) == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		return
	}

	//追加日志
	isChange := false
	for i, entry := range args.Entries {
		logicIdx := i + args.PrevLogIndex + 1
		// 如果 logicIdx 已经落入快照范围，跳过（或者报错，理论上不该发生）
		if logicIdx <= rf.LastIncludedIndex {
			continue
		}
		phyIdx := rf.getPhysicIdx(logicIdx)
		if phyIdx < len(rf.logs) {
			//如果索引范围内已经有日志了，检查任期
			if rf.logs[phyIdx].Term != entry.Term {
				//如果追加日志的位置的任期和leader日志的位置的任期不相等
				//将idx下标前面的日志进行切片保留
				rf.logs = rf.logs[:phyIdx]
				rf.logs = append(rf.logs, entry)
				isChange = true
			}
			//如果任期一样，说明这一段已经同步过了，下一条
		} else {
			//超出本地的日志长度，直接追加
			rf.logs = append(rf.logs, entry)
			isChange = true
		}
	}
	if isChange {
		rf.persist()
	}

	// 更新 CommitIndex：须用「当前日志最后一条」与 LeaderCommit 取 min。
	// 心跳时 len(Entries)==0，若仍用 PrevLogIndex+0 会小于 getLastIndex()，导致 commit 永远追不上 Leader。
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 处理回复（任期更新、调整 nextIndex 等）
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		// 2. 状态检查
		if rf.state != Leader || rf.currentTerm != args.Term {
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
				flag := false
				for i := args.PrevLogIndex; i >= rf.LastIncludedIndex; i-- {
					if rf.getTermByIndex(i) == reply.ConflictTerm {
						flag = true
						rf.nextIndex[server] = i + 1
						break
					}
				}
				if !flag {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}

		}
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { //旧 term 直接 return
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	//过时快照
	if rf.LastIncludedIndex >= args.LastIncludedIndex {
		return
	}
	// 截断日志
	// 如果快照涵盖的范围我本地也有（即 args.LastIncludedIndex 在我当前的 logs 范围内）
	// 我们保留该索引之后的日志，因为它们可能还没被包含在快照里
	if args.LastIncludedIndex < rf.getLastIndex() {
		pIdx := rf.getPhysicIdx(args.LastIncludedIndex)
		rf.logs = rf.logs[pIdx:] // 保留从 LastIncludedIndex 开始的后缀
	} else {
		// 快照比我整个日志都新，直接清空，只留一个占位符
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}

	// 更新快照元数据
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm

	// 持久化状态和快照数据
	rf.saveStateAndSnapshot(args.Data)

	// 通知上层应用快照：commitIndex 至少到快照末尾（已提交前缀）
	// 注意：不能在这里把 lastApplied 提到 LastIncludedIndex，否则 applier 会以为
	// 索引 1..LastIncludedIndex 已通过 Command 应用，从而跳过发送；同时若仍在向
	// applyCh 发送未发完的日志条目，会与快照乱序。lastApplied 只在 applier 里
	// 在真正把快照发到 applyCh 之后更新。
	if rf.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.LastIncludedIndex
	}

	rf.pendingSnapshot = raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.hasPendingSnapshot = true
	rf.applyCond.Broadcast()
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.state != Leader || rf.currentTerm != args.Term {
			return ok
		}
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.updateCommitIndex()
	}
	return ok
}

package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//  "bytes"
	//"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//  "6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 50 * time.Millisecond

// 投票状态枚举
type VoteState int

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //该Raft节点已终止
	Expire                  //投票(消息\竞选者）过期
	Voted                   //本Term内已经投过票
)

// 固定超时时间枚举
const (
	HeartbeatInterval  = 50 * time.Millisecond  // 心跳间隔，固定
	ElectionTimeoutMin = 150 * time.Millisecond // 选举超时下限
	ElectionTimeoutMax = 300 * time.Millisecond // 选举超时上限
	RPCTimeout         = 100 * time.Millisecond // RPC 超时，固定
)

// 枚举节点状态
type PeerState int

const (
	Follower  PeerState = iota //追随者
	Candidate                  //候选者
	Leader                     //领导者
)

// 日志条目
type LogEntry struct {
	Term    int         //任期
	Command interface{} //命令
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 互斥锁保护共享访问
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有节点的RPC端点
	persister *tester.Persister   // Object to hold this peer's persisted state 持久化状态
	me        int                 // this peer's index into peers[] 当前节点在peers中的索引
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state    PeerState  //当前节点状态 3A
	term     int        //当前任期 3A
	votedFor int        //当前任期投票给谁 3A
	votenums int        //当前节点获取到的票数 3A
	logs     []LogEntry //日志 3A 下标0的内容是占位的，真正的第一条命令在下标1

	//所有节点共享的不稳定状态
	commitIndex int //已提交的日志索引
	lastApplied int //已应用的日志索引

	//leader的不稳定状态 leader专用
	nextIndex  []int //下一个要发送的日志索引
	matchIndex []int //已匹配的日志索引

	lastHeartBeatTime time.Time     //最后一次心跳时间 3A
	electionTimeout   time.Duration //当前选举超时时间 3A

	applyChan chan raftapi.ApplyMsg // 用来写入应用消息的通道

	dead int32 //节点是否死亡

	applyCond *sync.Cond // 用于唤醒 applier
}

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

// example RequestVote RPC reply structure.
// field names must start with capital letters!

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

// -------------------------------
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 创建raft节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	//初始化状态和日志
	rf.state = Follower //初始化时为追随者
	rf.term = 0
	rf.votedFor = -1
	rf.votenums = 0
	rf.logs = []LogEntry{{Term: 0, Command: nil}} //对0下标占位
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastHeartBeatTime = time.Now()
	rf.electionTimeout = rf.randElectionTimeout()
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier() //apply 日志到状态机 3A 简单阻塞，3B 再写
	//rand.Seed(time.Now().UnixNano() + int64(rf.me)) //初始化随机种子 /在 Go 1.20 之后，全局生成器默认被自动种子化
	return rf

}

// example RequestVote RPC handler.
// 请求投票接口
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] RequestVote from candidate=%d argsTerm=%d myTerm=%d", rf.me, args.CandidateId, args.Term, rf.term)
	if args.Term < rf.term {
		DPrintf("[%d] RequestVote reject: argsTerm %d < myTerm %d", rf.me, args.Term, rf.term)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}
	if args.Term >= rf.term {
		//在任期相等时不应该更新任期并重置投票
		//在同一任期内，如果有多个 Candidate，
		// 它们会因为收到对方的投票请求而互相降级，
		// 导致谁都选不上，甚至出现逻辑死循环
		if args.Term > rf.term {
			rf.term = args.Term
			rf.votedFor = -1 //重置投票
		}
		rf.state = Follower
	}
	//检查候选人日志是否和我的一样新
	isuptodate := rf.isLogUpToDate(args)
	//没有投票或者已经投票给这个候选人并且候选人日志是否和我的一样新时
	//投票给他，重置选举计时器
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isuptodate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartBeatTime = time.Now()
		DPrintf("[%d] RequestVote grant to %d term=%d", rf.me, args.CandidateId, rf.term)
	} else {
		reply.VoteGranted = false
		DPrintf("[%d] RequestVote reject: votedFor=%d or !isUpToDate", rf.me, rf.votedFor)
	}
	reply.Term = rf.term

}

// ----------------------------------ticker------------------------------------------
// 选举超时ticker
func (rf *Raft) ticker() {
	for !rf.Killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		//time.Sleep(30 * time.Millisecond) //30ms检查一次,太频繁，导致rpc调用过多最后失败
		time.Sleep(15 * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		if state == Leader {
			// Leader 固定频率发心跳
			if time.Since(rf.lastHeartBeatTime) >= HeartBeatTimeout {
				rf.lastHeartBeatTime = time.Now()
				rf.mu.Unlock()
				rf.broadcastHeartbeat()
			} else {
				rf.mu.Unlock()
			}
		} else {
			// Follower/Candidate 逻辑：检查是否选举超时
			elapsed := time.Since(rf.lastHeartBeatTime)
			timeout := rf.electionTimeout
			rf.mu.Unlock()
			if elapsed >= timeout {
				DPrintf("[%d] ticker: 选举超时 触发 startElection state=%d", rf.me, state)
				rf.startElection()
			}
		}
	}

}

// 广播心跳 3B要加上日志
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	curTerm := rf.term
	//遍历所有节点，发送心跳
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIdx := rf.nextIndex[i]
		prevIndex := nextIdx - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		args := AppendEntriesArgs{
			Term:         curTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,               // 心跳用 leader 最后一条日志索引
			PrevLogTerm:  rf.logs[prevIndex].Term, // 对应任期
			Entries:      make([]LogEntry, len(rf.logs)-nextIdx),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, rf.logs[nextIdx:]) // 发送从 nextIdx 开始的所有日志

		// 发送 RPC
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	}

}

// ---------------------------------------election---------------------------------
// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader { //如果是leader，返回
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate //切换到候选人
	rf.term++            //任期++
	rf.votedFor = rf.me  //投票给自己
	rf.votenums = 1
	DPrintf("[%d] startElection: 转为 Candidate term=%d", rf.me, rf.term)
	rf.lastHeartBeatTime = time.Now()             //更新最后的心跳时间
	rf.electionTimeout = rf.randElectionTimeout() // 每次竞选都要重置随机时间
	//记录当前 term 和自己的 lastLogIndex/Term，
	// 拷出来放在局部变量，防止 RPC 回来时 term 已变
	curTerm := rf.term
	lastlogindex := len(rf.logs) - 1
	lastlogterm := rf.logs[lastlogindex].Term
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         curTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastlogindex,
				LastLogTerm:  lastlogterm,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			res := rf.sendRequestVote(server, &args, &reply)
			if res { //请求投票返回成功
				rf.mu.Lock()
				//如果收到任期比自己大的节点的回复
				if reply.Term > args.Term {
					if reply.Term > rf.term {
						rf.term = reply.Term
					}
					rf.state = Follower
					rf.votedFor = -1
					rf.votenums = 0
					DPrintf("[%d] startElection: 收到更大 term 退回 Follower from=%d replyTerm=%d", rf.me, server, reply.Term)
					rf.mu.Unlock()
					return
				}
				//判断自己是否还是竞选者，且任期不冲突
				if rf.state != Candidate || args.Term < rf.term {
					rf.mu.Unlock()
					return
				}
				//获得投票
				if reply.VoteGranted {
					rf.votenums++
					if (rf.votenums >= (len(rf.peers)/2 + 1)) && (rf.state == Candidate) {
						//条件满足，变为leader
						rf.state = Leader
						rf.votedFor = -1
						rf.votenums = 0
						DPrintf("[%d] startElection: 当选 Leader term=%d votes=%d", rf.me, rf.term, rf.votenums+1)
						// 初始化 nextIndex 和 matchIndex
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.logs)
						}
						rf.mu.Unlock() //先解锁再发送心跳
						// 发送心跳
						rf.broadcastHeartbeat()
					} else {
						rf.mu.Unlock()
						return
					}
				} else {
					rf.mu.Unlock()
					return
				}
			}
		}(i)
	}

}

// 追加日志到状态机
func (rf *Raft) applier() {
	for !rf.Killed() {
		rf.mu.Lock()
		// for rf.commitIndex <= rf.lastApplied { //不断检查
		// 	rf.mu.Unlock()
		// 	time.Sleep(10 * time.Millisecond)
		// 	rf.mu.Lock()
		// }

		// 如果没有新日志需要应用，就挂起等待 3B时优化sleep为条件变量
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait() // Wait 会自动释放锁并阻塞
		}

		//已提交的日志索引小于已引用的日志索引
		// 收集待应用的日志，注意不要在持锁时直接发送到 applyChan
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()

		// 在锁外发送到通道，防止由于通道阻塞导致整个 Raft 锁死
		for i, entry := range entries {
			rf.applyChan <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
		}

		rf.mu.Lock()
		// 更新 lastApplied 时要小心，确保不会被覆盖
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}

}

// 追加条目rpc，也用来发送心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] AppendEntries from L%d term=%d, my term=%d state=%d", rf.me, args.LeaderId, args.Term, rf.term, rf.state)
	reply.Success = false
	reply.Term = rf.term

	// 发现任期比自己小
	if args.Term < rf.term {
		DPrintf("[%d] AppendEntries: 拒绝 term 更小 leader=%d argsTerm=%d myTerm=%d", rf.me, args.LeaderId, args.Term, rf.term)
		return
	}

	// 发现更高任期或来自合法 Leader 的心跳
	rf.lastHeartBeatTime = time.Now()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		DPrintf("[%d] step down to Follower due to AE term=%d", rf.me, rf.term)
	}
	rf.state = Follower

	//一致性检查
	//日志长度不够
	if len(rf.logs) <= args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.logs) //告诉leader从我的末尾开始尝试
		return
	}

	//PrevLogIndex处任期不匹配
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term //更新发生冲突的任期
		idx := args.PrevLogIndex
		//从最新的日志位置开始向前找，找到冲突任期的下标
		//告诉leader，这个下标是冲突任期的下标，下一步继续找冲突位置，若没有则进行同步
		for idx > 0 && rf.logs[idx].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	//追加日志
	for i, entry := range args.Entries {
		idx := i + args.PrevLogIndex + 1
		if idx < len(rf.logs) {
			//如果索引范围内已经有日志了，检查任期
			if rf.logs[idx].Term != entry.Term {
				//如果追加日志的位置的任期和leader日志的位置的任期不相等
				//将idx下标前面的日志进行切片保留
				rf.logs = rf.logs[:idx]
				rf.logs = append(rf.logs, entry)
			}
			//如果任期一样，说明这一段已经同步过了，下一条
		} else {
			//超出本地的日志长度，直接追加
			rf.logs = append(rf.logs, entry)
		}
	}

	//更新CommitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		rf.applyCond.Broadcast() // 通知 applier 协程  开始忘加
	}
	reply.Success = true
}

// ---------------------------------persist----------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()

}

// ---------------------------------snapshot--------------------------------------
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //获取当前任期和是否是领导者
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == Leader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Killed() { //如果挂了
		return -1, -1, false
	}
	term := rf.term
	isLeader := false
	if rf.state != Leader { //不是leader
		return -1, -1, false
	}
	index := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	isLeader = true
	//rf.persist()//持久化 3C
	return index, term, isLeader
}

//---------------------------------dead--------------------------------------

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1 //1表示死亡，0表示存活

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

}

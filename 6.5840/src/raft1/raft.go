package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	//"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// func init() {
// 	// 测试中 Start(command) 的 command 为 int；经 RPC/gob 解码后若变成 int64，
// 	// 会与 tester 里保存的 int 不相等，导致 one() 永远等不到 agreement。
// 	labgob.Register(int(0))
// }

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
	state       PeerState  //当前节点状态 3A
	currentTerm int        //当前任期 3A
	votedFor    int        //当前任期投票给谁 3A
	votenums    int        //当前节点获取到的票数 3A
	logs        []LogEntry //日志 3A 下标0的内容是占位的，真正的第一条命令在下标1 | 3D 之后，rf.logs[0] 不再是 null 或者 0，它存储的是上一次快照最后一条日志的信息

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

	LastIncludedIndex int //快照最后一个日志索引 3D
	LastIncludedTerm  int //快照最后一个日志任期 3D

	// InstallSnapshot / 恢复时需在 Command 之前交给 applyChan（单消费者保序）
	pendingSnapshot    raftapi.ApplyMsg
	hasPendingSnapshot bool
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
	rf.currentTerm = 0
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
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 若从磁盘恢复了快照：只提高 commitIndex，lastApplied 仍为 0，由 applier 先发 SnapshotValid
	// 再更新 lastApplied。切勿在此处同步 applyChan<- 且同时 lastApplied==commitIndex==L：
	// applier 会先看到 commitIndex<=lastApplied 从而在 applyCond 上 Wait，而无人从 channel 收快照，Make 死锁。
	if rf.LastIncludedIndex > 0 {
		rf.commitIndex = rf.LastIncludedIndex
		rf.pendingSnapshot = raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.LastIncludedTerm,
			SnapshotIndex: rf.LastIncludedIndex,
		}
		rf.hasPendingSnapshot = true
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier() //apply 日志到状态机 3A 简单阻塞，3B 再写
	//rand.Seed(time.Now().UnixNano() + int64(rf.me)) //初始化随机种子 /在 Go 1.20 之后，全局生成器默认被自动种子化
	return rf
}

// 上层（KVServer）调用的主动截断函数
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 未提交的前缀不能截断（讲义：index 不能大于 commitIndex）
	if index > rf.commitIndex {
		return
	}
	//已经做过快照了
	if index <= rf.LastIncludedIndex {
		return
	}
	//先获取index在logs中的索引
	targetIdx := rf.getPhysicIdx(index)
	rf.LastIncludedTerm = rf.logs[targetIdx].Term

	//将logs更新为快照后的版本，记录关键信息
	newLogs := make([]LogEntry, len(rf.logs)-targetIdx)
	copy(newLogs, rf.logs[targetIdx:])
	newLogs[0].Command = nil // 占位符不需要 Command
	rf.logs = newLogs
	rf.LastIncludedIndex = index
	//确保截断后的日志和快照字节流一起落盘
	rf.saveStateAndSnapshot(snapshot)
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
	curTerm := rf.currentTerm
	//遍历所有节点，发送心跳
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIdx := rf.nextIndex[peer]
		if nextIdx <= rf.LastIncludedIndex {
			go rf.pushInstallSnapshot(peer)
			continue
		}
		prevIndex := nextIdx - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		prevTerm := rf.getTermByIndex(prevIndex)

		args := AppendEntriesArgs{
			Term:         curTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex, // 心跳用 leader 最后一条日志索引
			PrevLogTerm:  prevTerm,  // 对应任期
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		//获取物理下标
		physicStart := rf.getPhysicIdx(nextIdx)
		if physicStart < len(rf.logs) {
			args.Entries = make([]LogEntry, len(rf.logs)-physicStart)
			copy(args.Entries, rf.logs[physicStart:]) // 发送从 physicStart 开始的所有日志
		}

		// 发送 RPC
		go rf.sendAppendEntries(peer, &args, &AppendEntriesReply{})
	}

}

// 把 Leader 现有的快照完整地发给 Follower，让 Follower 直接跳级到快照所在的位置
func (rf *Raft) pushInstallSnapshot(peer int) {
	data := rf.persister.ReadSnapshot()
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              data,

		Done:   true,
		Offset: 0,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	rf.sendInstallSnapshot(peer, &args, &reply)

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
	rf.currentTerm++     //任期++
	rf.votedFor = rf.me  //投票给自己
	rf.persist()
	rf.votenums = 1
	DPrintf("[%d] startElection: 转为 Candidate term=%d", rf.me, rf.currentTerm)
	rf.lastHeartBeatTime = time.Now()             //更新最后的心跳时间
	rf.electionTimeout = rf.randElectionTimeout() // 每次竞选都要重置随机时间
	//记录当前 term 和自己的 lastLogIndex/Term，
	// 拷出来放在局部变量，防止 RPC 回来时 term 已变
	curTerm := rf.currentTerm
	lastlogindex := rf.getLastIndex() //3D修改
	lastlogterm := rf.getTermByIndex(lastlogindex)
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
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
					}
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.votenums = 0
					DPrintf("[%d] startElection: 收到更大 term 退回 Follower from=%d replyTerm=%d", rf.me, server, reply.Term)
					rf.mu.Unlock()
					return
				}
				//判断自己是否还是竞选者，且任期不冲突
				if rf.state != Candidate || args.Term < rf.currentTerm {
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
						rf.persist()
						rf.votenums = 0
						DPrintf("[%d] startElection: 当选 Leader term=%d votes=%d", rf.me, rf.currentTerm, rf.votenums+1)
						// 初始化 nextIndex 和 matchIndex
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						lastIx := rf.getLastIndex()
						for i := range rf.nextIndex {
							rf.nextIndex[i] = lastIx + 1
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
		if rf.hasPendingSnapshot {
			msg := rf.pendingSnapshot
			rf.hasPendingSnapshot = false
			snapIdx := msg.SnapshotIndex
			rf.mu.Unlock()
			rf.applyChan <- msg
			rf.mu.Lock()
			if snapIdx > rf.lastApplied {
				rf.lastApplied = snapIdx
			}
			rf.applyCond.Broadcast()
			rf.mu.Unlock()
			continue
		}

		// 若有待安装快照，即使 commitIndex==lastApplied 也必须醒来处理快照；
		// 否则仅 Broadcast 设置了 hasPendingSnapshot 时，条件仍为 commitIndex<=lastApplied，会永远 Wait。
		for rf.commitIndex <= rf.lastApplied && !rf.hasPendingSnapshot {
			rf.applyCond.Wait()
		}

		if rf.hasPendingSnapshot {
			rf.mu.Unlock()
			continue
		}

		nextIdx := rf.lastApplied + 1
		if nextIdx > rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		phyIdx := rf.getPhysicIdx(nextIdx)
		if phyIdx < 0 || phyIdx >= len(rf.logs) {
			rf.mu.Unlock()
			continue
		}
		entry := rf.logs[phyIdx]
		rf.mu.Unlock()

		rf.applyChan <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: nextIdx,
		}

		rf.mu.Lock()
		if nextIdx > rf.lastApplied {
			rf.lastApplied = nextIdx
		}
		rf.mu.Unlock()
	}

}

// ---------------------------------persist----------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// sanpshot前的版本
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	snap := rf.persister.ReadSnapshot() //先读取快照
	rf.persister.Save(raftstate, snap)  //再持久化状态和快照

}

// snapshot版本
func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vorfor int
	var logs []LogEntry
	var lastincludedindex int
	var lastincludedterm int
	if d.Decode(&term) != nil ||
		d.Decode(&vorfor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastincludedindex) != nil ||
		d.Decode(&lastincludedterm) != nil {
		DPrintf("readPersist err")
	} else {
		rf.currentTerm = term
		rf.votedFor = vorfor
		rf.logs = logs
		rf.LastIncludedIndex = lastincludedindex
		rf.LastIncludedTerm = lastincludedterm
	}

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //获取当前任期和是否是领导者
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	if rf.Killed() {
		rf.mu.Unlock()
		return -1, -1, false
	}
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	term := rf.currentTerm
	index := rf.getLastIndex() + 1
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	rf.persist()
	rf.mu.Unlock()
	//Leader 刚追加了一条日志，立刻再推一轮 RPC，不用干等下面 ticker 的 50ms 心跳周期，复制会快一点
	go rf.broadcastHeartbeat() //广播心跳
	return index, term, true
}

//---------------------------------dead--------------------------------------

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1 //1表示死亡，0表示存活
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

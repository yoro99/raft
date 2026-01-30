package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储中，以便在发生崩溃并重启后能够恢复。
// 关于哪些状态需要持久化，请参见论文中的 Figure 2。
// 在你尚未实现快照（snapshot）之前，应当向 persister.Save() 的第二个参数传入 nil。
// 在你实现了快照之后，传入当前的快照（如果还没有快照，则传入 nil）。
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

// PersistBytes how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot 服务通知 Raft：它已经创建了一个快照，该快照包含了直到并包括 index 在内的所有信息。
// 这意味着服务不再需要 index（含）之前的日志。 Raft 现在应当尽可能多地裁剪（截断）自己的日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = Invalid
	}
	if rf.currentTerm == args.Term && rf.votedFor == Invalid && checkLogFromRequestVote(&rf.logs, args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		go rf.timer(ElectionTimeout) // 同意后重制timer，如果来一个request就重制可能一直check不通过永远无法变为candidate
	} else {
		reply.VoteGranted = false
	}
	DPrintf("RequestVote me:%d me.term:%d candidate:%d candidate.term:%d  isVote::%t",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
}

// 向某个服务器发送 RequestVote RPC 的示例代码。
// server 是目标服务器在 rf.peers[] 中的索引。 args 中包含 RPC 的参数。 *reply 用于接收 RPC 的返回结果，因此调用者应当传入 &reply。
// 传递给 Call() 的 args 和 reply 的类型，必须与RPC 处理函数中声明的参数类型完全一致（包括是否为指针）。
//
// labrpc 包模拟的是一个有丢包的网络环境，在这种环境下，服务器可能无法访问，请求和响应也可能会丢失。
// Call() 会发送一个请求并等待响应。如果在超时时间内收到了响应，Call() 返回 true；否则返回 false。因此Call()可能会阻塞一段时间。
// 返回 false 的原因可能包括： 服务器已经宕机、服务器仍然存活但无法连通、请求丢失，或者响应丢失。
// 除非服务器端的 RPC 处理函数本身没有返回，否则 Call() 保证最终会返回（可能会有延迟）。 因此**不需要**在Call()外面自己实现超时机制。
// 更多细节请查看 ../labrpc/labrpc.go 中的注释。
//
// 如果你在让 RPC 正常工作时遇到问题，
// 请检查：1）通过 RPC 传输的结构体中，所有字段名是否都以大写字母开头；2）调用方是否使用 & 传入 reply 结构体的地址，而不是直接传结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start 使用 Raft 的服务（例如一个 k/v 服务器）希望开始就下一条将要追加到 Raft 日志中的命令达成一致。
// 如果该服务器不是 leader，则返回 false； 否则启动一致性过程并立即返回。
// 并不保证该命令最终一定会被提交到 Raft 日志中，因为 leader 可能会失败或在选举中失去领导权。
// 即使该 Raft 实例已经被 Kill，该函数也应当能够正常返回（而不是崩溃）。
//
// 第一个返回值是该命令如果最终被提交时，在日志中所处的索引位置；第二个返回值是当前的 term；第三个返回值表示该服务器是否认为自己是 leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	return index, term, isLeader
}

// Kill 测试程序在每个测试结束后并不会停止 Raft 创建的 goroutine，但会调用 Kill() 方法。
// 你的代码可以通过 killed() 来检查 Kill() 是否已经被调用。使用 atomic 可以避免加锁。
// 问题在于：长期运行的 goroutine 会占用内存，并可能持续消耗 CPU 时间，进而导致后续测试失败，同时还会产生令人困惑的调试输出。
// 任何包含长期运行循环的 goroutine，都应该调用 killed() 来判断自己是否需要停止运行。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// todo: 有循环操作才需要？
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) timer(timerType int) {
	switch timerType {
	case ElectionTimeout:
		rf.electionTimeoutTimerCount++ // yw: 用于重制计时器
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.electionTimeoutChan <- 1
	case HeartBeat: // todo 需要重制？
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.heartbeatChan <- 1
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			rf.electionTimeoutTimerCount--
			count := rf.electionTimeoutTimerCount
			rf.mu.Unlock()
			DPrintf("ticker electionTimeout peer-%d state:%d term:%d rf.electionTimeoutTimerCount: %d",
				rf.me, rf.state, rf.currentTerm, rf.electionTimeoutTimerCount)
			if count != 0 {
				continue
			}
			go rf.handleElectionTimeout()
		case <-rf.heartbeatChan:
			DPrintf("ticker heartbeat me:%d state:%d", rf.me, rf.state)
			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) handleElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("handleElectionTimeout me: %d, len(peers): %d", rf.me, len(rf.peers))
	rf.currentTerm++
	voteCount := 1
	rf.state = Candidate
	rf.votedFor = rf.me
	go rf.timer(ElectionTimeout)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: getLastLogIndex(&rf.logs),
			LastLogTerm:  getLastLogTerm(&rf.logs),
		}
		reply := RequestVoteReply{}
		go func(peer int) {
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// todo: if reply.Term == rf.currentTerm
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = Invalid
					return
				}
				if rf.state == Candidate {
					if reply.VoteGranted {
						voteCount++
					}
					if voteCount+voteCount > len(rf.logs) {
						rf.state = Leader
						go rf.broadcastHeartbeat()
					}
					DPrintf("after sendRequestVote me:%d state:%d reply:%t voteCount:%d",
						rf.me, rf.state, reply.VoteGranted, voteCount)
				}
			}
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Leader {
		return
	}
	DPrintf("broadcastHeartbeat %d", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PrevLogTerm:  getTermFromLogs(rf.nextIndex[peer]-1, &rf.logs),
			Entries:      getLogs(&rf.logs, rf.nextIndex[peer]-1), // todo：发送心跳时如果可以携带log会携带嘛？
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go func(peer int) {
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("after sendAppendEntries me:%d, peer:%d reply.term:%d reply.success:%t rf.nextIndex[peer]:%d",
					rf.me, peer, reply.Term, reply.Success, rf.nextIndex[peer]-1)
				if rf.state == Leader {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = Invalid
					} else if reply.Term == rf.currentTerm && !reply.Success { // todo:?
						if rf.nextIndex[peer] > 0 {
							rf.nextIndex[peer]-- // todo 处理log 不重试，等待下一次heartbeat
						}
					}
				}
			}
		}(peer)
	}
	go rf.timer(HeartBeat)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId // todo: 转为follower需要知道leader吗？
		reply.Success = false
	} else if args.Term < rf.currentTerm {
		reply.Success = false
	} else if !checkIndexFromAppendEntries(&rf.logs, args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
	} else {
		reply.Success = true
	}
	go rf.timer(ElectionTimeout)
}

// Make 服务或测试程序希望创建一个 Raft 服务器。
// 所有 Raft 服务器（包括当前这个）的端口都存放在 peers[] 中；当前服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序都是一致的。
// persister 用于保存该服务器的持久化状态，并且在初始化时如果存在已保存的状态，也会一并提供。
// applyCh 是一个通道，测试程序或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此任何耗时较长的工作都应当放到 goroutine 中执行。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	DPrintf("make a raft instance begin")
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		state:                     Follower,
		currentTerm:               0,
		votedFor:                  Invalid,
		logs:                      make([]logEntry, 0), // todo: index从1开始
		commitIndex:               0,
		lastApplied:               0,
		nextIndex:                 make([]int, len(peers)),
		matchIndex:                make([]int, len(peers)),
		heartbeatChan:             make(chan int),
		electionTimeoutChan:       make(chan int),
		electionTimeoutTimerCount: 0, // yw:用于重制选举超时计时器
	}
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.nextIndex {
		rf.nextIndex[i], rf.matchIndex[i] = 0, 0
	}
	// 一开始应该只要启动选举超时计时器，选出leader再开始发送心跳
	go rf.timer(ElectionTimeout)
	go rf.ticker()
	DPrintf("make a raft instance end")
	return rf
}

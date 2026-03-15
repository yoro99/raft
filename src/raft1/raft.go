package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	term = rf.currentTerm
	return term, isLeader
}

// 将 Raft 的持久化状态保存到稳定存储中，以便在发生崩溃并重启后能够恢复。
// 关于哪些状态需要持久化，请参见论文中的 Figure 2。
// 在你尚未实现快照（snapshot）之前，应当向 persister.Save() 的第二个参数传入 nil。
// 在你实现了快照之后，传入当前的快照（如果还没有快照，则传入 nil）。
// todo: snapshot传nil时别把之前的snap覆盖了！
func (rf *Raft) persist(snapshot []byte) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(rf.currentTerm) != nil ||
		encoder.Encode(rf.votedFor) != nil ||
		encoder.Encode(rf.lastIncludedIndex) != nil ||
		encoder.Encode(rf.lastIncludedTerm) != nil ||
		encoder.Encode(len(rf.logs)) != nil {
		panic("Persist can't encode currentTerm|votedFor|logs")
	}
	for i := 0; i < len(rf.logs); i++ {
		if encoder.Encode(rf.logs[i]) != nil {
			panic(fmt.Sprintf("Persist can't encode log[%d]", i))
		}
	}
	raftState := buffer.Bytes()

	currentSnapshot := snapshot
	if snapshot == nil {
		currentSnapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftState, currentSnapshot)
}

// restore previously persisted state.
// snapShot只需要写，不需要特殊处理读！！，可以直接读取
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm, voteFor, lastIncludeIndex, lastIncludeTerm, logLen int
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&voteFor) != nil ||
		decoder.Decode(&lastIncludeIndex) != nil ||
		decoder.Decode(&lastIncludeTerm) != nil ||
		decoder.Decode(&logLen) != nil {
		panic("readPersist can't decode some attributes: currentTerm|voteFor|logLen")
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm =
		currentTerm, voteFor, lastIncludeIndex, lastIncludeTerm

	var log LogEntry
	logs := make([]LogEntry, 0)
	for i := 0; i < logLen; i++ {
		if decoder.Decode(&log) != nil {
			panic("readPersist can't decode some LogEntry")
		}
		logs = append(logs, log)
	}
	rf.logs = logs
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex, _ := rf.getFirstLogInfo()
	lastIndex, _ := rf.getLastLogInfo()
	dPrintf("Snapshot before me:%d index:%d firstIndex:%d lastIndex:%d len(logs):%d", rf.me, index, firstIndex, lastIndex, len(rf.logs))
	if firstIndex > index {
		panic(fmt.Sprintf("Snapshot index error, index:%d, firstIndex:%d", index, firstIndex))
	}
	rf.logs = rf.shinkLogs(index)
	rf.persist(snapshot)
	dPrintf("Snapshot after me:%d len(logs):%d commitIndex:%d lastApplied:%d lastIncludedIndex:%d lastIncludedTerm:%d len(snapshot):%d len(readSnap):%d",
		rf.me, len(rf.logs), rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm, len(snapshot), len(rf.persister.ReadSnapshot()))
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	go rf.timer(ElectionTimeout)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = Follower
		rf.persist(nil)
	}
	// me：leader的commitIndex肯定最大，不用判断是当前任期
	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}
	lastIndex, _ := rf.getLastLogInfo()
	if lastIndex <= args.LastIncludedIndex {
		// todo：这逻辑也太不优雅了
		rf.logs = make([]LogEntry, 1)
		rf.commitIndex, rf.lastApplied = args.LastIncludedIndex, args.LastIncludedIndex
		rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
		rf.logs[0].Index, rf.logs[0].Term = rf.lastIncludedIndex, rf.lastIncludedTerm // todo: snapshot所有数据的情况
	} else {
		rf.shinkLogs(args.LastIncludedIndex)
	}

	rf.persist(args.Data)
	// todo: 可能提交后服务层会触发snapshot刷新状态，处理逻辑这边不需要写
	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = Invalid
		dPrintf("RequestVote product a follower:%d term:%d args.term:%d leaderId:%d======", rf.me, rf.currentTerm, args.Term, args.CandidateId)
	}
	if rf.votedFor == Invalid && checkLogFromRequestVote(&rf.logs, args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		go rf.timer(ElectionTimeout) // 同意后重制timer，如果来一个request就重制可能一直check不通过永远无法变为candidate
	} else {
		reply.VoteGranted = false
	}
	//dPrintf("RequestVote me:%d me.term:%d candidate:%d candidate.term:%d  isVote::%t",
	//	rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VoteGranted)
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
// 即使该 Raft 实例已经被 Kill，该函数也应当能够正常返回（而不是崩溃）。 todo:返回false
// 第一个返回值是该命令如果最终被提交时，在日志中所处的索引位置；第二个返回值是当前的 term；第三个返回值表示该服务器是否认为自己是 leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.Killed() == false {
		isLeader = rf.state == Leader
		if isLeader {
			rf.logs = append(rf.logs, LogEntry{
				Entry: command,
				Term:  rf.currentTerm,
				Index: getLastLogIndex(&rf.logs) + 1,
			})
			index = getLastLogIndex(&rf.logs)
			rf.matchIndex[rf.me] = index // me:!!！适配checkCommit
			for peer := range rf.replicatorChanList {
				if peer == rf.me {
					continue
				}
				rf.replicatorChanList[peer].Signal()
			}
		}
	}
	// Your code here (3B).
	dPrintf("Start command me:%d index:%d, term: %d, isLeader: %t, len(logs):%d cmd:%v",
		rf.me, index, term, isLeader, len(rf.logs), command)
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

// me: 有循环操作才需要
func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) timer(timerType int) {
	switch timerType {
	case ElectionTimeout:
		rf.electionTimeoutTimerCount++ // yw: 用于重制计时器
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.electionTimeoutChan <- 1
	case HeartBeat: // todo 需要重制？
		time.Sleep(time.Duration(80) * time.Millisecond) // 10ms会导致TestCount3B报错RPC counts aren't too high
		rf.heartbeatChan <- 1
	}
}

func (rf *Raft) ticker() {
	for rf.Killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			rf.electionTimeoutTimerCount--
			count := rf.electionTimeoutTimerCount
			rf.mu.Unlock()
			if count != 0 {
				continue
			}
			go rf.handleElectionTimeout()
		case <-rf.heartbeatChan:
			// dPrintf("ticker heartbeat me:%d state:%d", rf.me, rf.state)
			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) handleElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	dPrintf("ticker handleElectionTimeout peer-%d state:%d term:%d rf.electionTimeoutTimerCount: %d",
		rf.me, rf.state, rf.currentTerm, rf.electionTimeoutTimerCount)
	if rf.state == Leader { // 需要检查，否则可能会在生成leader后又重制状态导致3B用例leaderFailure异常
		return
	}
	rf.currentTerm++
	rf.state, rf.votedFor = Candidate, rf.me
	voteCount := 1
	go rf.timer(ElectionTimeout)

	lastIndex, lastTerm := rf.getLastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		reply := RequestVoteReply{}
		go func(peer int) {
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist(nil)
				dPrintf("RequestVoteReply me:%d, currentTerm:%d reply.term:%d, voteGranted:%v",
					rf.me, rf.currentTerm, reply.Term, reply.VoteGranted)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = Invalid
					return
				}
				if rf.state == Candidate { // me：心跳和状态改变只触发一次
					if reply.VoteGranted {
						voteCount++
					}
					if voteCount+voteCount > len(rf.peers) {
						rf.state = Leader
						lastIndex, _ := rf.getLastLogInfo()
						for idx := range rf.nextIndex {
							rf.nextIndex[idx], rf.matchIndex[idx] = lastIndex+1, 0 // todo: have a bug
						}
						go rf.broadcastHeartbeat()
						dPrintf("Product ==Leader== after sendRequestVote me:%d term:%d state:%d reply:%t voteCount:%d",
							rf.me, rf.currentTerm, rf.state, reply.VoteGranted, voteCount)
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	reply.Term = rf.currentTerm
	dPrintf("AppendEntries tmp debug me:%d currentTerm:%d args.Term:%d len(log):%d",
		rf.me, rf.currentTerm, args.Term, len(rf.logs))
	if args.Term < rf.currentTerm { // me: 无效rpc无需重制选举超时时间
		reply.Success = false
		return
	}
	go rf.timer(ElectionTimeout)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId // todo: 转为follower需要知道leader吗？我感觉需要
	}
	lastIndex, lastTerm := rf.getLastLogInfo()
	firstIndex, _ := rf.getFirstLogInfo() // getFirstIndex
	// todo: snapShot待适配
	if lastIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastIndex
		reply.ConflictTerm = 0
		reply.Success = false
		return
	} else if !rf.enableAppend(args.PrevLogIndex, args.PrevLogTerm, lastIndex, lastTerm, firstIndex) {
		_, reply.ConflictTerm = rf.getLogInfo(args.PrevLogIndex - firstIndex + 1)
		i := args.PrevLogIndex - firstIndex + 1
		// 注意第0个元素的处理（否则如果冲突第一个index是1会跳过return）！！！会导致rejoin of partitioned leader异常
		for ; i > 0 && rf.logs[i].Term == reply.ConflictTerm; i-- {
		}
		reply.ConflictIndex = i // todo：使leader跳过该冲突term的所有entry
		dPrintf("AppendEntries reject me:%d first:%d CTerm:%d, CIdx:%d", rf.me, firstIndex, reply.ConflictTerm, reply.ConflictIndex)
		reply.Success = false
		return
	}
	// todo: 处理gc
	if firstIndex == rf.logs[0].Index {
		rf.logs = append(rf.logs[:1], args.Entries...) // 特殊处理logs为空的初始情况
	} else {
		rf.logs = append(rf.logs[:args.PrevLogIndex-firstIndex+1+1], args.Entries...)
	}
	lastIndex, lastTerm = rf.getLastLogInfo()
	rf.matchIndex[rf.me] = lastIndex
	dPrintf("AppendEntries after me:%d lastIndex:%d lastTerm:%d commitIndex:%d leaderCommitIndex:%d lastApplied:%d now_len(log):%d len(args.log):%d prevIndex:%d, prevTerm:%d",
		rf.me, lastIndex, lastTerm, rf.commitIndex, args.LeaderCommit, rf.lastApplied, len(rf.logs), len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
	if args.LeaderCommit > rf.commitIndex && lastTerm == rf.currentTerm { // 需要检查任期？ 直接唤醒即可！！！
		rf.commitIndex = min(args.LeaderCommit, lastIndex)
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) applyWorker() {
	for rf.Killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			dPrintf("applyWorker me:%d commitIndex:%d, lastApplied:%d", rf.me, rf.commitIndex, rf.lastApplied)
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		firstIndex, _ := rf.getFirstLogInfo()
		logsNum, beginIndex := commitIndex-lastApplied, lastApplied+1
		logsCopy := make([]LogEntry, logsNum)
		copy(logsCopy, rf.getLogCopy(beginIndex+1-firstIndex, logsNum)) // 拷贝！
		lastIndex, lastTerm := rf.getLastLogInfo()
		dPrintf("applyWork copy logs me:%d len:%d lenCopy:%d lastIndex:%d lastTerm:%d", rf.me, len(rf.logs), len(logsCopy), lastIndex, lastTerm)
		rf.mu.Unlock()
		for i := range logsCopy {
			dPrintf("applyWorker the copied log me:%d idx:%d term:%d", rf.me, logsCopy[i].Index, logsCopy[i].Term)
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				CommandIndex: logsCopy[i].Index,
				CommandTerm:  logsCopy[i].Term, // yw: ab4
				Command:      logsCopy[i].Entry,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(commitIndex, rf.lastApplied) // todo: snopShot预埋
		dPrintf("applyWorker me:%d lastApplied %d -> %d", rf.me, lastApplied, rf.lastApplied)
		rf.mu.Unlock()

	}
}

func (rf *Raft) replicatorWorker(peer int) {
	rf.replicatorChanList[peer].L.Lock() // todo：使用wait之前一定要先上锁！！！那感觉可以共享一个mutex啊
	defer rf.replicatorChanList[peer].L.Unlock()
	for rf.Killed() == false {
		for rf.enableReplicate(peer) {
			rf.replicatorChanList[peer].Wait()
		}

		dPrintf("replicatorWorker readily to replicate me:%d, peer:%d", rf.me, peer)
		rf.replicateOneRound(peer) // 需要等待返回再开始下一轮 **模型
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Killed() || rf.state != Leader {
		return
	}
	dPrintf("broadcastHeartbeat leader=%d", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateOneRound(peer) // 不需要等待返回
	}
	go rf.timer(HeartBeat)
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	firstIndex, _ := rf.getFirstLogInfo()
	if (rf.nextIndex[peer] <= rf.lastIncludedIndex) && rf.nextIndex[peer]-1 != 0 { // todo：check 4D 改了nextIndex的条件可以过snapshot的第一个
		args, reply := rf.genInstallSnapshotParams()
		dPrintf("replicateOneRound before sendInstallSnapshot me:%d, peer:%d  args.Term:%d rf.nextIndex[peer]:%d firstIndex:%d LastIncludeIndex:%d LastIncludeTerm:%d hasData:%v len(snapshot):%d",
			rf.me, peer, args.Term, rf.nextIndex[peer], firstIndex, args.LastIncludedIndex, args.LastIncludedTerm, args.Data == nil, len(args.Data))

		rf.mu.Unlock()
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.handleInstallSnapshot(peer, args, reply)
		}
	} else {
		args, reply := rf.genAppendEntriesParams(peer)
		dPrintf("replicateOneRound before sendAppendEntries me:%d, peer:%d len(log): %d len(all log):%d args.Term:%d args.preIndex:%d args.preTerm:%d rf.nextIndex[peer]:%d lastLogIndex:%d",
			rf.me, peer, len(args.Entries), len(rf.logs), args.Term, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer], getLastLogIndex(&rf.logs))
		rf.mu.Unlock()
		if rf.sendAppendEntries(peer, args, reply) {
			rf.handleAppendEntries(peer, args, reply)
		}
	}
}

// Make 服务或测试程序希望创建一个 Raft 服务器。
// 所有 Raft 服务器（包括当前这个）的端口都存放在 peers[] 中；当前服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序都是一致的。
// persister 用于保存该服务器的持久化状态，并且在初始化时如果存在已保存的状态，也会一并提供。
// applyCh 是一个通道，测试程序或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此任何耗时较长的工作都应当放到 goroutine 中执行。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	// dPrintf("make a raft instance begin")
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		state:                     Follower,
		currentTerm:               0,
		votedFor:                  Invalid,
		logs:                      make([]LogEntry, 1), // todo: index从1开始, 2是len也是cap，会有初始值
		commitIndex:               0,
		lastApplied:               0,
		nextIndex:                 make([]int, len(peers)),
		matchIndex:                make([]int, len(peers)),
		heartbeatChan:             make(chan int),
		electionTimeoutChan:       make(chan int),
		electionTimeoutTimerCount: 0, // yw:用于重制选举超时计时器
		replicatorChanList:        make([]*sync.Cond, len(peers)),
	}
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex // todo: 适配3D
	rf.applyCond = sync.NewCond(&rf.mu)                                         // cond在wait后会自动释放其持有的锁，&sync.Mutex{}赋值方式！
	lastIndex, _ := rf.getLastLogInfo()
	// me:peer从0开始，log的index从1开始
	for peer := range rf.nextIndex {
		if peer == me {
			continue
		}
		rf.nextIndex[peer], rf.matchIndex[peer] = lastIndex+1, 0

		rf.replicatorChanList[peer] = sync.NewCond(&sync.Mutex{})
		go rf.replicatorWorker(peer)
	}
	// 一开始应该只要启动选举超时计时器，选出leader再开始发送心跳
	go rf.timer(ElectionTimeout)
	go rf.ticker()
	go rf.applyWorker()
	// dPrintf("make a raft instance end")
	return rf
}

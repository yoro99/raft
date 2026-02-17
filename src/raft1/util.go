package raft

import (
	"fmt"
	"log"
	"slices"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf("====DEBUG=== "+format, a...)
	}
}

// todo: 这样带来的性能优化多吗
func getTermFromLogs(idx int, logs *[]LogEntry) int {
	// 如果当前选中的leader的logs被刷新导致丢失了nextIndex的数据
	// todo：思考需要特殊处理吗？
	if idx >= len(*logs) {
		return 0
	}
	return (*logs)[idx].Term
}

func getLastLogIndex(logs *[]LogEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].Index
}

func getLogs(logs *[]LogEntry, idx int) []LogEntry {
	res := make([]LogEntry, 0)
	for i := len(*logs) - 1; i >= idx; i-- {
		res = append(res, (*logs)[i])
	}
	// DPrintf("getLogs len:%d idx:%d res_len:%d", len(*logs), idx, len(res))
	slices.Reverse(res)
	return res
}

func checkLogFromRequestVote(logs *[]LogEntry, lastLogIndex int, lastLogTerm int) bool {
	if len(*logs) == 1 {
		return true
	}
	lastLog := (*logs)[len(*logs)-1]
	if lastLogTerm > lastLog.Term {
		return true
	}
	if lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index {
		return true
	}
	return false
}

func (rf *Raft) getLogInfo(idx int) (Index, Term int) {
	if len(rf.logs) <= idx || idx < 0 {
		Index, Term = 0, 0
		return
	}
	Index, Term = rf.logs[idx].Index, rf.logs[idx].Term
	return
}

func (rf *Raft) getLastLogInfo() (int, int) {
	return rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLogCopy(left, n int) []LogEntry {
	//i, l := 1, len(rf.logs)
	//for ; i < l && rf.logs[i].Index != left; i++ {
	//}
	//if i+n > l {
	//	return make([]logEntry, 0)
	//}
	DPrintf("getLogCopy left:%d left+n:%d len:%d", left, left+n, len(rf.logs))

	if left+n > len(rf.logs) {
		panic(fmt.Sprintf("getLogCopy error left+n:%d len:%d", left+n, len(rf.logs)))
	}
	return rf.logs[left : left+n]
}

func (rf *Raft) enableAppend(preLogIndex, preLogTerm, lastIndex, lastTerm, firstIndex int) bool {
	DPrintf("enableAppend debug1 me:%d preLogIndex:%d, perLogTerm:%d, lastIndex:%d lastTerm:%d firstIndex:%d",
		rf.me, preLogIndex, preLogTerm, lastIndex, lastTerm, firstIndex)
	if preLogIndex == lastIndex && preLogTerm == lastTerm { //todo: delete
		return true
	}
	//fmt.Printf("test me:%d len:%d, pre:%d findex:%d\n", rf.me, len(rf.logs), preLogIndex, firstIndex)
	_, term := rf.getLogInfo(preLogIndex + 1 - firstIndex)
	DPrintf("enableAppend debug2 me:%d preLogIndex:%d, perLogTerm:%d, lastIndex:%d lastTerm:%d firstIndex:%d term:%d",
		rf.me, preLogIndex, preLogTerm, lastIndex, lastTerm, firstIndex, term)
	if term == preLogTerm {
		return true
	}
	return false
}

func (rf *Raft) checkCommit(idx int) {
	l, r := rf.commitIndex, idx

	for l <= r {
		mid, num := (l+r)>>1, 0
		for peer := range rf.matchIndex {
			if rf.matchIndex[peer] >= mid {
				num += 1
			}
		}
		//DPrintf("checkCommit begin me:%d idx:%d l:%d, r:%d mid:%d res:%t",
		//	rf.me, idx, l, r, mid, num >= len(rf.matchIndex)/2+1)

		if num >= len(rf.matchIndex)/2+1 {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}

	if r <= rf.commitIndex {
		return
	}
	firstIndex, _ := rf.getLogInfo(1)
	_, term := rf.getLogInfo(r - firstIndex + 1)
	DPrintf("checkout me:%d commit:%d len(log):%d r:%d term:%d cTerm:%d firstIdex:%d",
		rf.me, rf.commitIndex, len(rf.logs), r, term, rf.currentTerm, firstIndex)
	if term == rf.currentTerm {
		rf.commitIndex = idx
		rf.applyCond.Signal()
		DPrintf("checkCommit me:%d signal", rf.me)
	}
}

func (rf *Raft) enableReplicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex, _ := rf.getLastLogInfo()
	return rf.matchIndex[peer] >= lastIndex || rf.state != Leader //todo: 和教程对比
}

func (rf *Raft) shinkLogs(index int) []LogEntry {
	logs := make([]LogEntry, 0)
	firstIndex, _ := rf.getLogInfo(1)
	lastIndex, _ := rf.getLastLogInfo()
	if index < lastIndex {
		copy(logs, rf.logs[index+1-firstIndex+1:])
	}
	// todo: 确定？
	rf.commitIndex, rf.lastApplied = max(rf.commitIndex, index), max(rf.lastApplied, index)
	rf.lastIncludedIndex, rf.lastIncludedTerm = rf.logs[index-firstIndex+1].Index, rf.logs[index-firstIndex+1].Term
	return logs
}

func (rf *Raft) genAppendEntriesParams(peer int) (*AppendEntriesArgs, *AppendEntriesReply) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1, // todo:check in 3B
		PrevLogTerm:  getTermFromLogs(rf.nextIndex[peer]-1, &rf.logs),
		Entries:      getLogs(&rf.logs, rf.nextIndex[peer]), // todo：发送心跳时如果可以携带log会携带嘛？
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	return &args, &reply
}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	DPrintf("replicateOneRound after sendAppendEntries me:%d, peer:%d len(log): %d len(all log):%d args.Term:%d args.preIndex:%d args.preTerm:%d reply.term:%d currTerm: %d reply.success:%t rf.nextIndex[peer]:%d lastLogIndex:%d reply.ConflictIndex:%d",
		rf.me, peer, len(args.Entries), len(rf.logs), args.Term, args.PrevLogIndex, args.PrevLogTerm, reply.Term, rf.currentTerm, reply.Success, rf.nextIndex[peer], getLastLogIndex(&rf.logs), reply.ConflictIndex)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = Invalid
	}
	if rf.state == Leader && rf.currentTerm == args.Term { // me: 改变值时需要确保是当前任期发送的数据！
		if reply.Success {
			rf.matchIndex[peer] = getLastLogIndex(&args.Entries)
			rf.nextIndex[peer] = getLastLogIndex(&args.Entries) + 1 // 需要是rf的最后一个日志，而不是当前的args.logs的最后一个（可能是空，3B最后一个案例）
			rf.checkCommit(getLastLogIndex(&args.Entries))
		} else {
			// todo 处理log 不重试，等待下一次heartbeat
			rf.nextIndex[peer] = reply.ConflictIndex + 1 // todo：conflictIndex充分检查，不+1会导致no agreement if too many followers disconnect 报错[-1]
		}
	}
}

func (rf *Raft) genInstallSnapshotParams() (*InstallSnapshotArgs, *InstallSnapshotReply) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	return &args, &reply
}

func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = Invalid
		rf.persist(nil)
	}
}

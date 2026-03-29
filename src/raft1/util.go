package raft

import (
	"fmt"
	"log"
	"os"
	"slices"
)

// Debugging
const debug = false

var firstLog bool = false

func dPrintf(format string, a ...interface{}) {
	if debug {
		if !firstLog {
			f, _ := os.Create("raft.log")
			log.SetOutput(f)
			firstLog = true
		}
		log.Printf("====DEBUG raft=== "+format, a...)
	}
}

func getLastLogIndex(logs *[]LogEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].Index
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

func (rf *Raft) getLogs(idx int) []LogEntry {
	res := make([]LogEntry, 0)

	for i := len(rf.logs) - 1; i >= 1 && rf.logs[i].Index >= idx; i-- {
		res = append(res, rf.logs[i])
	}
	// dPrintf("getLogs len:%d idx:%d res_len:%d", len(*logs), idx, len(res))
	slices.Reverse(res)
	return res
}

func (rf *Raft) getTermFromLogs(idx int) int {
	// 如果当前选中的leader的logs被刷新导致丢失了nextIndex的数据
	// todo：思考需要特殊处理吗？
	firstIndex, firstTerm := rf.getFirstLogInfo()
	// if firstIndex == 0 && rf.logs[0].Index == idx {
	if firstIndex == idx {
		return firstTerm
	}
	if idx-firstIndex+1 >= len(rf.logs) || idx-firstIndex+1 < 0 {
		panic(fmt.Sprintf("getTermFromLogs has a error idx, idx:%d, firstIdx:%d targetIdx:%d len(logs):%d",
			idx, firstIndex, idx-firstIndex+1, len(rf.logs)))
	}
	return rf.logs[idx-firstIndex+1].Term
}

// todo：处理firstIndex!!!
func (rf *Raft) getFirstLogInfo() (Index, Term int) {
	if len(rf.logs) == 1 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	return rf.logs[1].Index, rf.logs[1].Term
}

func (rf *Raft) getLogInfo(idx int) (Index, Term int) {
	if len(rf.logs) <= idx || idx < 0 {
		Index, Term = 0, 0
		// panic(fmt.Sprintf("getLogInfo error len(log):%d, idx:%d", len(rf.logs), idx))
		dPrintf("getLogInfo error len(log):%d, idx:%d", len(rf.logs), idx)
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
	dPrintf("getLogCopy left:%d left+n:%d len:%d", left, left+n, len(rf.logs))

	if left+n > len(rf.logs) {
		panic(fmt.Sprintf("getLogCopy error left+n:%d len:%d", left+n, len(rf.logs)))
	}
	return rf.logs[left : left+n]
}

func (rf *Raft) enableAppend(preLogIndex, preLogTerm, lastIndex, lastTerm, firstIndex int) bool {
	dPrintf("enableAppend debug1 me:%d preLogIndex:%d, perLogTerm:%d, lastIndex:%d lastTerm:%d firstIndex:%d, len(logs):%d",
		rf.me, preLogIndex, preLogTerm, lastIndex, lastTerm, firstIndex, len(rf.logs))
	if preLogIndex == lastIndex && preLogTerm == lastTerm { //todo: delete
		return true
	}
	//fmt.Printf("test me:%d len:%d, pre:%d findex:%d\n", rf.me, len(rf.logs), preLogIndex, firstIndex)
	_, term := rf.getLogInfo(preLogIndex + 1 - firstIndex)
	dPrintf("enableAppend debug2 me:%d preLogIndex:%d, perLogTerm:%d, lastIndex:%d lastTerm:%d firstIndex:%d term:%d",
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
		//dPrintf("checkCommit begin me:%d idx:%d l:%d, r:%d mid:%d res:%t",
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
	firstIndex, _ := rf.getFirstLogInfo()
	_, term := rf.getLogInfo(r - firstIndex + 1)
	dPrintf("checkout me:%d commit:%d len(log):%d r:%d term:%d cTerm:%d firstIndex:%d",
		rf.me, rf.commitIndex, len(rf.logs), r, term, rf.currentTerm, firstIndex)
	if term == rf.currentTerm {
		rf.commitIndex = idx
		rf.applyCond.Signal()
		dPrintf("checkCommit me:%d signal", rf.me)
	}
}

func (rf *Raft) enableReplicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex, _ := rf.getLastLogInfo()
	return rf.matchIndex[peer] >= lastIndex || rf.state != Leader //todo: 和教程对比
}

// 将index之后的压缩
func (rf *Raft) shinkLogs(index int) []LogEntry {
	logs := make([]LogEntry, 1) // todo:需要从第一个开始
	firstIndex, _ := rf.getFirstLogInfo()
	lastIndex, _ := rf.getLastLogInfo()
	srcLog := make([]LogEntry, 0)
	if lastIndex > index {
		srcLog = rf.logs[index+1-firstIndex+1:]
	}
	if index < lastIndex {
		logs = append(logs, srcLog...) // todo：copy不能扩容？
	}
	dPrintf("shinkLogs index:%d firstIndex:%d lastIndex:%d beginIndex:%d len(newLog):%d len(oldLog):%d",
		index, firstIndex, lastIndex, index+1-firstIndex+1, len(logs), len(rf.logs))
	// todo: 确定？
	rf.commitIndex, rf.lastApplied = max(rf.commitIndex, index), max(rf.lastApplied, index)
	rf.lastIncludedIndex, rf.lastIncludedTerm = rf.logs[index-firstIndex+1].Index, rf.logs[index-firstIndex+1].Term
	logs[0].Index, logs[0].Term = rf.lastIncludedIndex, rf.lastIncludedTerm // todo: snapshot所有数据的情况
	return logs
}

func (rf *Raft) genAppendEntriesParams(peer int) (*AppendEntriesArgs, *AppendEntriesReply) {
	dPrintf("genAppendEntriesParams me:%d peer:%d, nextIndex:%d", rf.me, peer, rf.nextIndex[peer])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1, // todo:check in 3B
		PrevLogTerm:  rf.getTermFromLogs(rf.nextIndex[peer] - 1),
		Entries:      rf.getLogs(rf.nextIndex[peer]), // todo：发送心跳时如果可以携带log会携带嘛？
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	return &args, &reply
}

func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(nil)
	// todo：了解：因为在send期间心跳和start都会触发appendEntries，会导致内容重复
	rf.nextIndex[peer] = getLastLogIndex(&rf.logs) + 1 // todo：？？？位置是不是有问题？
	dPrintf("replicateOneRound after sendAppendEntries me:%d, peer:%d len(log): %d len(all log):%d args.Term:%d args.preIndex:%d args.preTerm:%d reply.term:%d currTerm: %d reply.success:%t rf.nextIndex[peer]:%d lastLogIndex:%d reply.ConflictIndex:%d",
		rf.me, peer, len(args.Entries), len(rf.logs), args.Term, args.PrevLogIndex, args.PrevLogTerm, reply.Term, rf.currentTerm, reply.Success, rf.nextIndex[peer], getLastLogIndex(&rf.logs), reply.ConflictIndex)
	// 需要是rf.logs的后一个日志，而不是当前的args.Entries的后一个（可能是空，3B最后一个案例；可能有报错too many RPC bytes）
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = Invalid
	}
	if rf.state == Leader && rf.currentTerm == args.Term { // me: 改变值时需要确保是当前任期发送的数据！
		if reply.Success {
			rf.matchIndex[peer] = getLastLogIndex(&args.Entries)
			rf.checkCommit(getLastLogIndex(&args.Entries))
		} else {
			// todo 处理log不重试，等待下一次heartbeat
			// todo: 处理ConflictTerm, ConflictIndex是下一个需要传的log
			// 前置条件1：共识算法，所有server前置任期肯定匹配，否则违背一致性（过不了一致性检查）
			// 前置条件2: 如果有冲突index，只要leader中存在term，肯定是follower的index太多了！！！，取道leader的conflictTerm最后一个index即可
			//		如果leader中没有term，则follower的整个conflictTerm无效，全部覆盖
			rf.nextIndex[peer] = reply.ConflictIndex // todo：conflictIndex充分检查，不+1会导致no agreement if too many followers disconnect 报错[-1]
			if reply.ConflictTerm != -1 {
				index := 0
				firstIndex, _ := rf.getFirstLogInfo()
				for i, _ := rf.getLastLogInfo(); i >= firstIndex; i-- {
					if rf.logs[i-firstIndex+1].Term == reply.ConflictTerm {
						index = i
						break
					}
				}
				if index > 0 {
					rf.nextIndex[peer] = index
				}
			}
		}
	}
}

func (rf *Raft) genInstallSnapshotParams() (*InstallSnapshotArgs, *InstallSnapshotReply) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	dPrintf("genInstallSnapshotParams me:%d len(snapshot):%d len(args.Data):%d",
		rf.me, len(rf.persister.ReadSnapshot()), len(args.Data))
	reply := InstallSnapshotReply{}
	return &args, &reply
}

func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dPrintf("replicateOneRound after sendInstallSnapshot me:%d, peer:%d args.Term:%d reply.term:%d currTerm: %d",
		rf.me, peer, args.Term, reply.Term, rf.currentTerm)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = Invalid
		rf.persist(nil)
		return
	}
	if rf.state == Leader && rf.currentTerm == args.Term {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	dPrintf("replicateOneRound end sendInstallSnapshot me:%d, peer:%d isLeader:%v nextIndex:%d, matchIndex:%d",
		rf.me, peer, rf.state == Leader, rf.nextIndex[peer], rf.matchIndex[peer])
}

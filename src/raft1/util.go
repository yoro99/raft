package raft

import (
	"fmt"
	"log"
	"slices"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf("====DEBUG=== "+format, a...)
	}
}

// todo: 这样带来的性能优化多吗
func getTermFromLogs(idx int, logs *[]logEntry) int {
	if idx <= len(*logs) {
		return 0
	}
	return (*logs)[idx].Term
}

// todo: go的数组是值传参？
func checkIndexFromAppendEntries(logs *[]logEntry, prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex == -1 {
		return true
	}
	if prevLogIndex > len(*logs) || ((*logs)[prevLogIndex].Term != prevLogTerm) {
		return false
	}
	return true
}

func getLastLogIndex(logs *[]logEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].Index
}

func getLastLogTerm(logs *[]logEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].Term
}

func getLogs(logs *[]logEntry, idx int) []logEntry {
	res := make([]logEntry, 0)
	for i := len(*logs) - 1; i >= idx; i-- {
		res = append(res, (*logs)[i])
	}
	DPrintf("getLogs len:%d idx:%d res_len:%d", len(*logs), idx, len(res))
	slices.Reverse(res)
	return res
}

func checkLogFromRequestVote(logs *[]logEntry, lastLogIndex int, lastLogTerm int) bool {
	if len(*logs) == 0 {
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

func (rf *Raft) getLogCopy(left, n int) []logEntry {
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
	if preLogIndex == lastIndex && preLogTerm == lastTerm { //todo: delete
		return true
	}
	//fmt.Printf("test me:%d len:%d, pre:%d findex:%d\n", rf.me, len(rf.logs), preLogIndex, firstIndex)
	_, term := rf.getLogInfo(preLogIndex + 1 - firstIndex)
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
	return rf.nextIndex[peer] > lastIndex
}

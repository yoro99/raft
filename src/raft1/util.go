package raft

import (
	"log"
	"slices"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// todo: 这样带来的性能优化多吗
func getTermFromLogs(idx int, logs *[]logEntry) int {
	if idx < 0 {
		return 0
	}
	return (*logs)[idx].term
}

// todo: go的数组是值传参？
func checkIndexFromAppendEntries(logs *[]logEntry, prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex == -1 {
		return true
	}
	if prevLogIndex > len(*logs) || ((*logs)[prevLogIndex].term != prevLogTerm) {
		return false
	}
	return true
}

func getLastLogIndex(logs *[]logEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].index
}

func getLastLogTerm(logs *[]logEntry) int {
	logLen := len(*logs)
	if logLen == 0 {
		return 0
	}
	return (*logs)[logLen-1].term
}

func getLogs(logs *[]logEntry, idx int) []interface{} {
	res := make([]interface{}, 0)
	for i := len(*logs) - 1; i > idx; i-- {
		res = append(res, (*logs)[i])
	}
	slices.Reverse(res)
	return res
}

func checkLogFromRequestVote(logs *[]logEntry, lastLogIndex int, lastLogTerm int) bool {
	if len(*logs) == 0 {
		return true
	}
	lastLog := (*logs)[len(*logs)-1]
	if lastLogTerm > lastLog.term {
		return true
	}
	if lastLogTerm == lastLog.term && lastLogIndex >= lastLog.index {
		return true
	}
	return false
}

package raft

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan raftapi.ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	logs        []logEntry // 这啥语法

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeatChan             chan int
	electionTimeoutChan       chan int
	electionTimeoutTimerCount int // new
}

type logEntry struct {
	entry interface{}
	term  int
	index int // todo：在snapshot之后会清0吗, 从1开始吗
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

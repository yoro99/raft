package raft

const (
	Invalid         = -1
	ElectionTimeout = iota
	HeartBeat

	Follower
	Candidate
	Leader
)

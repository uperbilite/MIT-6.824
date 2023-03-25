package raft

import (
	"math/rand"
	"time"
)

const (
	ElectionTimeoutStart = 800
	ElectionTimeoutEnd   = 1200
	HeartbeatTimeout     = 50
)

func getRandomTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(ElectionTimeoutEnd-ElectionTimeoutStart) + ElectionTimeoutStart
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) getFirstLogTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex >= len(rf.log) {
		prevLogIndex = rf.getLastLogIndex()
	}
	return rf.log[prevLogIndex].Index, rf.log[prevLogIndex].Term
}

func (rf *Raft) hasLogOfTerm(term int) bool {
	for _, entry := range rf.log {
		if entry.Term == term {
			return true
		}
	}
	return false
}

func (rf *Raft) getFirstLogIndexOfTerm(term int) int {
	for index, entry := range rf.log {
		if entry.Term == term {
			return index
		}
	}
	return -1
}

func (rf *Raft) getLastLogIndexOfTerm(term int) int {
	var result int
	for index, entry := range rf.log {
		if entry.Term == term {
			result = index
		}
	}
	return result
}

func (rf *Raft) getIndexOfConflictTerm(conflictTerm int) int {
	for i := rf.getLastLogIndex(); i > 0; i-- {
		term := rf.log[i].Term
		if term == conflictTerm {
			return i
		} else if term < conflictTerm {
			break
		}
	}
	return -1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

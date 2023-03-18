package raft

import (
	"math"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool // always true for now
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d %s] received heartbeat from leader: %d.\n", rf.me, rf.state, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	biggerTerm := int(math.Max(float64(args.Term), float64(rf.currentTerm)))

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = biggerTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm, rf.votedFor = biggerTerm, -1
	}

	rf.state = Follower
	reply.Term, reply.Success = biggerTerm, true
	rf.lastResetTime = time.Now()

	DPrintf("[%d %s] finish handling heartbeat from leader: %d.\n", rf.me, rf.state, args.LeaderId)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

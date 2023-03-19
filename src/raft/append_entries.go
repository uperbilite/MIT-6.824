package raft

import (
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
	DebugReceiveHB(rf.me, args.LeaderId, rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	biggerTerm := max(rf.currentTerm, args.Term)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = biggerTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		DebugToFollower(rf, biggerTerm)
		rf.state = Follower
		rf.currentTerm, rf.votedFor = biggerTerm, -1
	}

	// stay follower in response to heartbeat or append entries
	rf.state = Follower
	rf.lastResetTime = time.Now()

	reply.Term, reply.Success = biggerTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

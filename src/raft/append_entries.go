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
	DPrintf("[%d] received heartbeat from leader: %d.\n", rf.me, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	biggerTerm := int(math.Max(float64(args.Term), float64(rf.currentTerm)))

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = biggerTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
	}
	if rf.state == Candidate {
		rf.state = Follower
	}

	rf.lastResetTime = time.Now()

	DPrintf("[%d] finish handling heartbeat from leader : %d.\n", rf.me, args.LeaderId)
}

func (rf *Raft) CallAppendEntries(server int) bool {
	DPrintf("[%d] appending entries to %d.\n", rf.me, server)

	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, &args, &reply)
	DPrintf("[%d] finish appending entries to %d.\n", rf.me, server)
	if !ok {
		return false
	}
	return reply.Success
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

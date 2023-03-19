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

func (rf *Raft) hasPrevLog(prevLogIndex, prevLogTerm int) bool {
	for _, entry := range rf.log {
		if entry.Index == prevLogIndex && entry.Term == prevLogTerm {
			return true
		}
	}
	return false
}

func (rf *Raft) delConflictLog(entries []Entry) {
	if rf.log[entries[0].Index].Term != entries[0].Term {
		rf.log = rf.log[:entries[0].Index]
	}
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
	if !rf.hasPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = biggerTerm, false
		return
	}
	// not heartbeat, delete conflict log and append entries
	if len(args.Entries) != 0 {
		rf.delConflictLog(args.Entries)
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	if args.Term > rf.currentTerm {
		DebugToFollower(rf, biggerTerm)
		rf.state = Follower
		rf.currentTerm, rf.votedFor = biggerTerm, -1
	}
	// TODO: apply commit

	// stay follower in response to heartbeat or append entries
	rf.state = Follower
	rf.lastResetTime = time.Now()

	reply.Term, reply.Success = biggerTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

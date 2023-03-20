package raft

import "time"

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
		firstIndex := rf.getFirstLogIndex()
		for index, entry := range args.Entries {
			if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
				rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...)
				break
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.apply()
	}
	if args.Term > rf.currentTerm {
		DebugToFollower(rf, biggerTerm)
		rf.state = Follower
		rf.currentTerm, rf.votedFor = biggerTerm, -1
	}

	// stay follower in response to heartbeat or append entries
	rf.state = Follower
	reply.Term, reply.Success = biggerTerm, true

	rf.lastResetTime = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

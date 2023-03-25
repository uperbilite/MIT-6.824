package raft

import "time"

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatTimeout * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			DebugHB(rf.me, rf.currentTerm)
			rf.sendHeartbeat(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat(term int) {
	rf.lastResetTime = time.Now()
	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args = AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Entry{},
				LeaderCommit: rf.commitIndex,
			}
			// not heartbeat
			if rf.getLastLogIndex() >= rf.nextIndex[server] {
				args.Entries = make([]Entry, rf.getLastLogIndex()-rf.nextIndex[server]+1)
				copy(args.Entries, rf.log[rf.nextIndex[server]-rf.getFirstLogIndex():])
			}
			rf.mu.Unlock()

			DebugSendingAppendEntries(rf, server, &args)
			if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader || rf.currentTerm != term {
				return
			}
			if reply.Term > rf.currentTerm {
				DebugToFollower(rf, reply.Term)
				rf.state = Follower
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
				return
			}
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DebugCommitSuccess(rf.me, server, term, rf.matchIndex[server], rf.nextIndex[server])
			} else {
				if reply.XTerm != -1 {
					if rf.hasLogOfTerm(reply.XTerm) {
						rf.nextIndex[server] = rf.getLastLogIndexOfTerm(reply.XTerm) + 1
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
				} else {
					rf.nextIndex[server] = rf.getLastLogIndex() - reply.XLen
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}
		}(server)
	}
	rf.updateCommitIndex(term)
	rf.apply()
}

func (rf *Raft) updateCommitIndex(term int) {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
		if rf.log[N].Term != term {
			continue
		}
		count := 1
		for i := 0; i < len(rf.matchIndex); i++ {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
			if count > len(rf.matchIndex)/2 {
				DebugUpdateCommitIdx(rf.me, term, rf.commitIndex, N)
				rf.commitIndex = N
			}
		}
	}
}

package raft

import (
	"time"
)

// The electionTicker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		nowTime := time.Now()
		time.Sleep(time.Duration(getRandomTimeout()) * time.Millisecond)
		rf.mu.Lock()
		if nowTime.After(rf.lastResetTime) && rf.state != Leader {
			rf.state = Candidate
			rf.currentTerm += 1
			DebugELT(rf.me, rf.currentTerm)
			rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection(term int) {
	rf.lastResetTime = time.Now()
	rf.votedFor = rf.me
	votes := 1
	rf.persist()

	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			var reply RequestVoteReply
			rf.mu.Unlock()

			DebugRequestVote(rf.me, server, rf.currentTerm)
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if reply.Term > rf.currentTerm {
				DebugToFollower(rf, reply.Term)
				rf.state = Follower
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
				return
			}
			if reply.VoteGranted {
				DebugGetVote(rf.me, server, rf.currentTerm)
				votes += 1
				if votes > len(rf.peers)/2 {
					DebugToLeader(rf, rf.currentTerm)
					rf.state = Leader
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					DebugHB(rf.me, rf.currentTerm)
					rf.sendHeartbeat(rf.currentTerm)
				}
				return
			}
		}(server)
	}
}

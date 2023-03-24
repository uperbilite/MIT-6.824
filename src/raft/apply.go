package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) apply() {
	DebugApply(rf.me, rf.currentTerm, rf.log)
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		firstIndex := rf.getFirstLogIndex()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])

		rf.mu.Unlock()
		DebugApplyCommit(rf.me, rf.currentTerm, rf.log)
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()

		rf.lastApplied = max(rf.lastApplied, commitIndex)

		rf.mu.Unlock()
	}
}

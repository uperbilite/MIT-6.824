package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"github.com/uperbilite/MIT-6.824/labgob"
	"time"

	"sync"
	"sync/atomic"

	"github.com/uperbilite/MIT-6.824/labrpc"
)

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

type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state         State
	lastResetTime time.Time
	applyCh       chan ApplyMsg
	applyCond     *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
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
	prevLogIndex := rf.matchIndex[server]
	return rf.log[prevLogIndex].Index, rf.log[prevLogIndex].Term
}

func (rf *Raft) startElection(term int) {
	rf.lastResetTime = time.Now()
	rf.votedFor = rf.me
	votes := 1

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
				return
			}
			if reply.VoteGranted {
				DebugGetVote(rf.me, server, rf.currentTerm)
				votes += 1
				if votes > len(rf.peers)/2 {
					DebugToLeader(rf, rf.currentTerm)
					rf.state = Leader
					DebugHB(rf.me, rf.currentTerm)
					rf.sendHeartbeat(rf.currentTerm)
				}
				return
			}
		}(server)
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
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]:]...)
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
				return
			}
			if reply.Success {
				DebugCommitSuccess(rf.me, server, term)
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				rf.nextIndex[server]--
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}
			rf.updateCommitIndex(term)
		}(server)
	}
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
				rf.apply()
			}
		}
	}
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DebugInfo("failed to load persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	if !rf.isLeader() {
		return -1, -1, false
	}

	rf.log = append(rf.log, Entry{
		Index:   rf.getLastLogIndex() + 1,
		Term:    rf.currentTerm,
		Command: command,
	})

	DebugCommand(rf.me, rf.currentTerm, rf.log)
	go rf.sendHeartbeat(rf.currentTerm)

	return rf.getLastLogIndex(), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The electionTicker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		nowTime := time.Now()
		time.Sleep(time.Duration(GetRandomTimeout()) * time.Millisecond)
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

func (rf *Raft) apply() {
	DebugApply(rf.me, rf.currentTerm, rf.log)
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if rf.commitIndex > rf.lastApplied && rf.getLastLogIndex() > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			DebugApplyCommit(rf.me, rf.currentTerm, rf.log)
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		state:         Follower,
		lastResetTime: time.Now(),
		applyCh:       applyCh,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]Entry, 1, 1),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers), len(peers)),
		matchIndex:    make([]int, len(peers), len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}

	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}

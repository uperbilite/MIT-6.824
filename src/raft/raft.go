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
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
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
	applyCond     sync.Cond

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

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) startElection(term int) {
	rf.votedFor = rf.me
	votes := 1

	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
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
			if reply.VoteGranted {
				DebugGetVote(rf.me, server, rf.currentTerm)
				votes += 1
				if votes > len(rf.peers)/2 {
					DebugToLeader(rf.me, votes, rf.currentTerm)
					rf.state = Leader
					rf.sendHeartbeat(rf.currentTerm)
				}
			} else if reply.Term > rf.currentTerm {
				DebugToFollower(rf, reply.Term)
				rf.state = Follower
				rf.currentTerm, rf.votedFor = reply.Term, -1
			}
		}(server)
	}
}

func (rf *Raft) sendHeartbeat(term int) {
	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			var reply AppendEntriesReply
			DebugSendingAppendEntries(rf, server, &args)
			rf.mu.Unlock()

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
			}
		}(server)
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	index := len(rf.log)
	term, isLeader := rf.GetState()

	// Your code here (2B).
	go rf.sendHeartbeat(term)

	return index, term, isLeader
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
			rf.lastResetTime = time.Now()
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
			rf.lastResetTime = time.Now()
			DebugHB(rf.me, rf.currentTerm)
			rf.sendHeartbeat(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	for _ = range rf.applyCh {
		// TODO: conditional wait.
		rf.applyCond.Wait()
	}
	rf.mu.Unlock()
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
		log:           nil,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     nil,
		matchIndex:    nil,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}

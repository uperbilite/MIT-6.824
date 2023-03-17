package raft

import (
	"log"
	"math"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("[%d] received request vote from %d.\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	biggerTerm := int(math.Max(float64(args.Term), float64(rf.currentTerm)))

	if args.Term == rf.currentTerm && rf.votedFor != -1 && args.CandidateId == rf.votedFor {
		reply.Term, reply.VoteGranted = biggerTerm, false
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = biggerTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = biggerTerm, -1
	}
	// TODO: compare log
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = biggerTerm, true
	rf.lastResetTime = time.Now()

	log.Printf("[%d] finish request vote from %d.\n", rf.me, args.CandidateId)
}

func (rf *Raft) CallRequestVote(server int) bool {
	log.Printf("[%d] sending request vote to %d.\n", rf.me, server)

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()
	var reply RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &reply)
	log.Printf("[%d] finish sending request vote to %d.\n", rf.me, server)
	if !ok {
		// A false return can be caused by a dead server, a live server that
		// can't be reached, a lost request, or a lost reply.
		return false
	}

	// TODO: return vote result
	return true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
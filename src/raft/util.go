package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

// send vote
func DebugGrantVote(s1, s2, term int) {
	Debug(dVote, "S%d >-VOTE-> S%d at T%d", s1, s2, term)
}

// receive vote
func DebugGetVote(s1, s2, term int) {
	Debug(dVote, "S%d <-VOTE-< S%d at T%d", s1, s2, term)
}

func DebugRequestVote(s1, s2, term int) {
	Debug(dVote, "S%d Request Vote From S%d at T%d", s1, s2, term)
}

// become to leader
func DebugToLeader(rf *Raft, newTerm int) {
	Debug(dLeader, "S%d [T%d:%s] -> [T%d:Leader]", rf.me, rf.currentTerm, rf.state, newTerm)
}

// become to follower
func DebugToFollower(rf *Raft, newTerm int) {
	Debug(dLeader, "S%d [T%d:%s] -> [T%d:Follower]", rf.me, rf.currentTerm, rf.state, newTerm)
}

// election timeout
func DebugELT(s, term int) {
	Debug(dTimer, "S%d Start Election for T%d", s, term)
}

func DebugHB(s, term int) {
	Debug(dTimer, "S%d Start Heartbeat at T%d", s, term)
}

func DebugSendingHB(s1, s2, term int) {
	Debug(dLog, "S%d >-HB-> S%d at T%d", s1, s2, term)
}

func DebugReceiveHB(s1, s2, term int) {
	Debug(dLog, "S%d <-HB-< S%d at T%d", s1, s2, term)
}

func DebugSendingAppendEntries(rf *Raft, server int, args *AppendEntriesArgs) {
	Debug(dLog, "S%d T:%d -> S%d Sending PLI: %d PLT: %d LC: %d LOG: %v",
		rf.me, rf.currentTerm, server, args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, args.Entries)
}

func DebugCommitSuccess(s1, s2, term, a, b int) {
	Debug(dCommit, "S%d <-CM-< S%d at T%d nextIndex: matchIndex", s1, s2, term, a, b)
}

func DebugUpdateCommitIdx(s, term, old, new int) {
	Debug(dCommit, "S%d T:%d CommitIdx From %d To %d", s, term, old, new)
}

func DebugApply(s, term int, log []Entry) {
	Debug(dClient, "S%d T:%d Apply Request, log: %v", s, term, log)
}

func DebugApplyCommit(s, term int, log []Entry) {
	Debug(dClient, "S%d T:%d Apply Commit, log: %v", s, term, log)
}

func DebugCommand(s, term int, log []Entry) {
	Debug(dCommit, "S%d T:%d Receive Command, log: %v", s, term, log)
}

func DebugInfo(s string, a ...interface{}) {
	Debug(dInfo, s, a...)
}

const (
	ElectionTimeout  = 150
	HeartbeatTimeout = 100
)

func GetRandomTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

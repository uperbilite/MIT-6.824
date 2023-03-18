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
func DebugToLeader(s, num, term int) {
	Debug(dLeader, "S%d is Leader(%d) for T%d", s, num, term)
}

// become to follower
func DebugToFollower(rf *Raft, newTerm int) {
	Debug(dTrace, "S%d [%s:%d] -> [Follower:%d]", rf.me, rf.state, rf.currentTerm, newTerm)
}

// election timeout
func DebugELT(s, term int) {
	Debug(dTimer, "S%d Start Election for T%d", s, term)
}

// receive heartbeat
func DebugReceiveHB(s1, s2, term int) {
	Debug(dTimer, "S%d <-HB-< S%d at T%d", s1, s2, term)
}

func DebugGetInfo(rf *Raft) {
	Debug(dInfo, "S%d T%d State: %s Log:%v", rf.me, rf.currentTerm, rf.state, rf.log)
}

func DebugSendingAppendEntries(rf *Raft, server int, args *AppendEntriesArgs) {
	Debug(dLog, "S%d T:%d -> S%d Sending PLI: %d PLT: %d LC: %d ",
		rf.me, rf.currentTerm, server, args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit)
}

func DebugTest(s string) {
	Debug(dTest, s)
}

const (
	ElectionTimeout  = 150
	HeartbeatTimeout = 50
)

func GetRandomTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}

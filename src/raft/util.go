package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ElectionTimeout  = 400
	HeartbeatTimeout = 300
)

func GetRandomTimeout() int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}

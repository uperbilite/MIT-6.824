package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ElectionTimeout  = 150
	HeartbeatTimeout = 50
)

func GetRandomTimeout() int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}

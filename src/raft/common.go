package raft

import "time"

const HeartBeatTimeout = time.Millisecond * time.Duration(150) // sleep time between successive AppendEntries call
const ElectionTimeout = time.Millisecond * time.Duration(1000)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	CommandTerm  int
	Command      interface{}
}

// struct definition for log entry
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type serverStatus int32

const (
	Live serverStatus = iota
	Dead
)

type Err string

const (
	OK         = "OK"
	ErrRPCFail = "ErrRPCFail"
)

type serverState int32

const (
	Leader serverState = iota
	Follower
	Candidate
)

func (state serverState) String() string {
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	default:
		return "Candidate"
	}
}

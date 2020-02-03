package raft

import "log"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's current term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term from other servers
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int        // leader's commit index
	Len          int        // number of logs sends to follower
	Entries      []LogEntry // logs that send to follower
}

type AppendEntriesReply struct {
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Term          int
	ConflictIndex int // in case of conflicting, follower include the first index it store for conflict term
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		rf.resetElectionTimerWithLock() // granting vote to candidate, reset timer
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // the server has voted in this term
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// detect higher term
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
		DPrintf("VOTE: Server %d receiving request vote from %d transition to follower, origin state: %s", rf.me, args.CandidateId, rf.state)
		rf.resetElectionTimerWithLock()                       // detect higher term via vote request trans to Follower
		rf.stateTransitionWithLock(Follower, Follower, false) // detect higher term via vote request trans to Follower
	}

	rf.leaderId = -1 // other server trying to elect a new leader
	reply.Term = args.Term
	lastLogIndex := rf.logIndex - 1
	if lastLogIndex == 0 && (rf.lastApplied != 0 || rf.commitIndex != 0) {
		log.Fatalf("ERROR: Server %d lastLogIndex is %d, rf.lastApplied: %d, rf.term: %d, rf.commitIndex: %d, entries: %v", rf.me, rf.logIndex, rf.lastApplied, rf.currentTerm, rf.commitIndex, rf.log)
	}
	if rf.log[lastLogIndex].LogTerm > args.LastLogTerm || // the server has log with higher term
		(rf.log[lastLogIndex].LogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // under same term, this server has longer index
		reply.VoteGranted = false
		DPrintf("VOTE: Server %d reject candidate %d vote, vote args: %v, server state: %s, last log index: %d, last log term: %d", rf.me, args.CandidateId, args, rf.state, lastLogIndex, rf.log[lastLogIndex].LogTerm)
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerWithLock() // granting vote to candidate, reset timer
	rf.persist()
	DPrintf("VOTE: Follower %d vote for candidate: %d, vote args: %v, follower term: %d, last log index: %d, last log term: %d", rf.me, args.CandidateId, args, rf.currentTerm, lastLogIndex, rf.log[lastLogIndex].LogTerm)
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term { // RPC call comes from an illegitimate leader
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} // else args.Term >= rf.currentTerm

	reply.Term = args.Term
	rf.leaderId = args.LeaderId
	rf.resetElectionTimerWithLock() // reset timer when there is a legal leader

	if args.Term > rf.currentTerm {
		rf.stateTransitionWithLock(Follower, Follower, false) // detect higher term via ApplyEntry rpc request
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.resetElectionTimerWithLock() // reset timer when detect higher term via ApplyEntry rpc request
		rf.persist()
	} else if args.Term == rf.currentTerm {
		rf.resetElectionTimerWithLock()
		rf.stateTransitionWithLock(Follower, Follower, false) // detect same term(which means there is a legal leader) via ApplyEntry rpc request
	}

	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	if logIndex <= prevLogIndex || rf.log[prevLogIndex].LogTerm != args.PrevLogTerm { // follower don't agree with leader on last log entry
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.log[conflictIndex].LogTerm
		for ; conflictIndex > rf.commitIndex && rf.log[conflictIndex-1].LogTerm == conflictTerm; conflictIndex-- {
		}

		reply.Success, reply.ConflictIndex = false, Max(rf.commitIndex+1, conflictIndex)
		return
	}
	reply.Success, reply.ConflictIndex = true, -1

	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.log[prevLogIndex+1+i].LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			rf.log = append(rf.log[:rf.logIndex]) // delete any conflicting log entries
			break
		}
	}
	if args.Len > 0 && i < args.Len {
		DPrintf("APPEND: Follower %d receive log from leader: %d, appending following logs: %v", rf.me, args.LeaderId, args.Entries[i:args.Len])
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	rf.persist()
	go rf.notifyApply()
}

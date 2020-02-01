package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxfy1992/6.824-2020/src/labgob"
	"github.com/maxfy1992/6.824-2020/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leaderId             int           // leader's id, initialized to -1
	currentTerm          int           // latest term server has seen, initialized to 0
	votedFor             int           // candidate that received vote in current term, initialized to -1
	commitIndex          int           // index of highest log entry known to be committed, initialized to 0
	lastApplied          int           // index of highest log entry known to be applied to state machine, initialized to 0
	state                serverState   // state of server
	status               serverStatus  // live or dead
	log                  []LogEntry    // log entries len()=5 (0,1,2,3,4 0 is no use) then logIndex=5
	logIndex             int           // index of next log entry to be stored, initialized to 1
	nextIndex            []int         // for each server, index of the next log entry to send to that server
	matchIndex           []int         // for each server, index of highest log entry, used to track committed index
	applyCh              chan ApplyMsg // apply to client
	notifyCh             chan struct{} // notify to apply
	timeOutElectionTimer *time.Timer   // timer used for timeout for election
}

// After a leader comes to power, it calls this function to initialize nextIndex and matchIndex
func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logIndex)
	e.Encode(rf.log)
	// TODO this two variables persistent ?
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)

	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&rf.log) != nil {
		//d.Decode(&commitIndex) != nil ||
		//d.Decode(&lastApplied) != nil {
		log.Fatal("Error in unmarshal raft state")
	}
	rf.currentTerm, rf.votedFor, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, logIndex, commitIndex, lastApplied
}

// send RequestVote RPC call to server and handle reply
func (rf *Raft) makeRequestVoteCall(server int, args *RequestVoteArgs, voteCh chan<- bool, retryCh chan<- int) {

	rf.mu.Lock()
	if rf.state != Candidate {
		go func() { voteCh <- false }() // stop election
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	if ok := rf.sendRequestVote(server, args, &reply); ok {
		if reply.VoteGranted {
			voteCh <- true
		} else { // since other server don't grant the vote, check if this server is obsolete
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				DPrintf("VOTE: When asking for vote, server %d find itself is obsolete, transition to follower, old term: %d, new term: %d", rf.me, rf.currentTerm, reply.Term)
				// TODO 这里可能存在问题，如何保证Follower timeout之前退出上一轮的选举？？
				rf.resetElectionTimer()
				rf.state, rf.currentTerm, rf.votedFor, rf.leaderId = Follower, reply.Term, -1, -1
				rf.persist()
				go func() { voteCh <- false }() // stop election
			}
			rf.mu.Unlock()
		} // else other server is more up-to-date this server
	} else {
		retryCh <- server
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.timeOutElectionTimer.Stop()
	rf.timeOutElectionTimer.Reset(newRandDuration(ElectionTimeout))
}

// leader nerver call startElection
// follower call startElection from bgElection
// Candidate call startElection from startElection
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	// start a new election
	rf.leaderId = -1    // server believes there is no leader
	rf.currentTerm += 1 // increment current term
	rf.votedFor = rf.me // vote for self
	currentTerm, lastLogIndex, me, serverCount := rf.currentTerm, rf.logIndex-1, rf.me, len(rf.peers)
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	rf.persist()
	rf.mu.Unlock()

	args := RequestVoteArgs{Term: currentTerm, CandidateId: me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

	voteCh := make(chan bool, serverCount-1)
	retryCh := make(chan int, serverCount-1)
	for i := 0; i < serverCount; i++ {
		if i != me {
			go rf.makeRequestVoteCall(i, &args, voteCh, retryCh)
		}
	}

	electionDuration := newRandDuration(ElectionTimeout)
	electionTimer := time.NewTimer(electionDuration) // in case there's no quorum, this election should timeout
	DPrintf("CANDIDATE: Candidate %d time out, start election and ask for vote, args: %d, rf.logIndex: %d, next timeout time: %s", me, args, lastLogIndex+1, electionDuration.String())

	voteCount, threshold := 0, serverCount/2 // counting vote

	for {
		select {
		case status := <-voteCh:
			if !status {
				// stop election because higher term from other servers
				return
			}
			voteCount += 1
			if voteCount >= threshold { // receive enough vote
				rf.mu.Lock()
				if rf.state == Candidate { // check if server is in candidate state before becoming a leader
					DPrintf("CANDIDATE: Server %d receive enough vote and becoming a new leader", rf.me)
					rf.state = Leader
					rf.initIndex() // after election, reinitialized nextIndex and matchIndex
					go rf.bgReplicateLog()
				} // if server is not in candidate state, then another server establishes itself as leader
				rf.mu.Unlock()
				return
			}
		case follower := <-retryCh:
			rf.mu.Lock()
			if rf.status == Live && rf.state == Candidate {
				go rf.makeRequestVoteCall(follower, &args, voteCh, retryCh)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				return
			}
		case <-electionTimer.C: // election timeout
			rf.mu.Lock()
			if rf.status == Live && rf.state == Candidate {
				go rf.startElection()
			}
			rf.mu.Unlock()
			return
		}
	}
}

// make append entries call to follower and handle reply
func (rf *Raft) makeAppendEntriesCall(follower int, retryCh chan<- int, empty bool) {
	var args AppendEntriesArgs
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.log[prevLogIndex].LogTerm
	if empty || rf.nextIndex[follower] == rf.logIndex {
		args = AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0, Entries: nil}
	} else {
		logs := rf.log[prevLogIndex+1:]
		args = AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: len(logs), Entries: logs}
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if ok := rf.sendAppendEntries(follower, &args, &reply); ok { // RPC call success
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm { // the leader is obsolete
				DPrintf("When appending entries, server %d find itself is obsolete, transition to follower, old term: %d, new term: %d", rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm, rf.state, rf.votedFor, rf.leaderId = reply.Term, Follower, -1, -1
				rf.resetElectionTimer()
				rf.persist()
			} else { // follower is inconsistent with leader
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
			}
		} else { // reply.Success is true
			prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
			if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] { // TODO ?in case apply arrive in out of order
				rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + logEntriesLen
				if logEntriesLen > 0 {
					DPrintf("Leader %d update follower %d next index: %d, match index: %d", rf.me, follower, rf.nextIndex[follower], rf.matchIndex[follower])
				}
			}
			// if log entry contains term equals to current term, then try if we can commit log by counting replicas
			if prevLogIndex+logEntriesLen < rf.logIndex && rf.commitIndex < prevLogIndex+logEntriesLen && rf.log[prevLogIndex+logEntriesLen].LogTerm == rf.currentTerm {
				l := len(rf.peers)
				threshold, count, agreedFollower := l/2, 0, make([]int, 0, l)
				for j := 0; j < l; j++ {
					if j != rf.me && rf.matchIndex[j] >= prevLogIndex+logEntriesLen {
						count += 1
						agreedFollower = append(agreedFollower, j)
					}
				}
				if count >= threshold {
					rf.commitIndex = prevLogIndex + logEntriesLen // can commit log
					rf.persist()
					go rf.notifyApply()
					DPrintf("Leader %d have following servers: %v replicating log and can update commit index to :%d", rf.me, agreedFollower, rf.commitIndex)
				}
			}
		}
		rf.mu.Unlock()
	} else { // retry
		retryCh <- follower
	}
}

// handle AppendEntries call fail, only sends heartbeat messages(empty log entry)
func (rf *Raft) bgRetryAppendEntries(retryCh chan int, done <-chan struct{}) {
	for {
		select {
		case follower := <-retryCh:
			go rf.makeAppendEntriesCall(follower, retryCh, true)
		case <-done:
			return
		}
	}
}

// replicate (empty)log to follower
func (rf *Raft) bgReplicateLog() {
	retryCh := make(chan int)
	done := make(chan struct{})
	go rf.bgRetryAppendEntries(retryCh, done)
	for {
		rf.mu.Lock()
		if rf.status != Live || rf.state != Leader {
			rf.mu.Unlock()
			done <- struct{}{}
			return
		}
		for follower := 0; follower < len(rf.peers); follower++ {
			if follower != rf.me {
				go rf.makeAppendEntriesCall(follower, retryCh, false)
			}
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatTimeout)
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader { // append log only if server is leader
		index := rf.logIndex
		entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
		DPrintf("Leader %d start new log entry: %v", rf.me, entry)
		if index != len(rf.log) {
			DPrintf("logIndex=(%d) len(rf.log)=(%d)", index, len(rf.log))
			panic("inconsistent logIndex and len(rf.log)")
		}
		rf.log = append(rf.log, entry)

		rf.matchIndex[rf.me] = rf.logIndex
		rf.logIndex += 1
		rf.persist()
		return index, rf.currentTerm, true
	} else {
		return -1, -1, false
	}
}

// notify to apply
func (rf *Raft) notifyApply() {
	rf.mu.Lock()
	logIndex, lastApplied, commitIndex := rf.logIndex, rf.lastApplied, rf.commitIndex
	rf.mu.Unlock()
	if lastApplied < logIndex && lastApplied < commitIndex {
		rf.notifyCh <- struct{}{}
	}
}

func (rf *Raft) bgApply() {
	// TODO quit
	for {
		select {
		case <-rf.notifyCh:
			rf.mu.Lock()
			startIndex, endIndex := rf.lastApplied+1, rf.commitIndex
			entries := append(rf.log[startIndex : endIndex+1])
			rf.lastApplied = rf.commitIndex
			rf.persist()
			rf.mu.Unlock()

			for i := 0; i < len(entries); i++ {
				// TODO maybe wait for receive
				rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
			}
		}
	}
}

func (rf *Raft) bgElection() {
	// TODO quit
	// 使用sleep有个问题无法解决，比如心跳成功后，应该继续延迟Timeout，有leader时不触发选举的逻辑，因此选择timer实现，注意好stop的使用
	for {
		//randomDuration := newRandDuration(ElectionTimeout)
		//time.Sleep(randomDuration)
		select {
		case <-rf.timeOutElectionTimer.C:
			rf.mu.Lock()
			if rf.state == Follower {
				rf.state = Candidate
				go rf.startElection() // follower timeout, start a new election
			}
			rf.mu.Unlock()
		}
	}
}

// main background
func (rf *Raft) runBgs() {
	// handle election timeout
	go rf.bgElection()

	// handle for apply entry to client state machine
	go rf.bgApply()
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
func Make(peers []*labrpc.ClientEnd, me int, persistent *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persistent
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logIndex = 1
	rf.state = Follower // initializing as follower
	rf.status = Live
	atomic.StoreInt32(&rf.dead, 0)
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.applyCh = applyCh
	rf.notifyCh = make(chan struct{})
	rf.timeOutElectionTimer = time.NewTimer(newRandDuration(ElectionTimeout))
	// initialize from state persisted before a crash
	rf.readPersist(persistent.ReadRaftState())

	go rf.runBgs()
	return rf
}

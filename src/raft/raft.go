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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	// debug log
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

const (
	follower int = iota
	candidate
	leader
)

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

type logEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	serversNum int
	persister  *Persister // Object to hold this peer's persisted state
	me         int        // this peer's index into peers[]
	dead       int32      // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	rwMu sync.RWMutex

	// temperary state to maintain timer
	timeStart time.Time
	leaderId  int

	// persistent state on all server
	currentTerm int
	votedFor    int
	logs        []logEntry

	// Volatile state on all server
	state        int
	commitIndex  int
	lastApplied  int
	lastLogIndex int

	// Volatile state on leaders
	// reinitialized after election
	nextIndex  []int
	matchIndex []int
}

const RaftElectionTimeoutAct = 1000 * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.rwMu.RLock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.rwMu.RUnlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rwMu.Lock()
	// if term < currentTerm, return false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d refuse to vote S%d because currentTerm=%d > Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.rwMu.Unlock()
		reply.VoteGranted = false
		return
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	lastLogTerm := rf.logs[rf.lastLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastLogIndex)) {
		if args.Term > rf.currentTerm {
			Debug(dVote, "S%d agree to vote S%d and turn into follower because currentTerm=%d < Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			rf.state = follower
		} else {
			Debug(dVote, "S%d agree to vote S%d currentTerm=%d Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		}
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.timeStart = time.Now()
		reply.VoteGranted = true
	} else {
		if args.Term > rf.currentTerm {
			Debug(dVote, "S%d rufuse to vote S%d but turn into follower because currentTerm=%d < Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			rf.votedFor = -1
			rf.leaderId = -1
			rf.state = follower
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			Debug(dVote, "S%d refuse to vote S%d because currentTerm=%d has voted for other\n", rf.me, args.CandidateId, rf.currentTerm)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
	rf.rwMu.Unlock()
}

func (rf *Raft) InitiateVote() {
	rf.rwMu.Lock()
	// if the sever has become leader, the vote is invaild
	if rf.state == leader {
		Debug(dVote, "S%d Vote interrupted because has become leader\n", rf.me)
		rf.rwMu.Unlock()
		return
	}
	rf.currentTerm++
	Debug(dVote, "S%d begin vote currentTerm=%d\n", rf.me, rf.currentTerm)
	rf.state = candidate
	rf.votedFor = rf.me
	// fill the args
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.logs[rf.lastLogIndex].Term}
	rf.rwMu.Unlock()
	rf.developVotes(&args)
}

func (rf *Raft) developVotes(args *RequestVoteArgs) {
	ch := make(chan *RequestVoteReply, rf.serversNum)
	// record wether the vote is successful
	var decision bool = false
	success := 1
	failure := 0
	var majority int = rf.serversNum/2 + 1
	// send voteRequest
	for i := 0; i < rf.serversNum; i++ {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendAndHandleRequestVote(i, args, &reply, ch)
	}
	// record the vote from other severs
loop:
	for {
		result := <-ch
		if result == nil {
			return
		}
		switch {
		case result.VoteGranted:
			success++
		case !result.VoteGranted:
			failure++
		}
		// make decision if one of results exceeds the majority
		switch {
		case success >= majority:
			decision = true
			break loop
		case failure >= majority:
			decision = false
			break loop
		}
	}
	// handle the final decision
	rf.rwMu.Lock()
	decision = decision && rf.currentTerm == args.Term && rf.state == candidate
	// fail the vote
	if !decision {
		Debug(dVote, "S%d Vote fails, currentTerm=%d state=%d\n", rf.me, rf.currentTerm, rf.state)
		rf.rwMu.Unlock()
		return
	}
	// succeed in the vote
	rf.state = leader
	rf.leaderId = rf.me
	for i := 0; i < rf.serversNum; i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	Debug(dVote, "S%d Vote succeeds, currentTerm=%d\n", rf.me, rf.currentTerm)
	rf.rwMu.Unlock()
}

func (rf *Raft) sendAndHandleRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan *RequestVoteReply) {
	ok := false
	for !ok {
		if rf.killed() {
			ch <- nil
			return
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.rwMu.Lock()
	if args.Term != rf.currentTerm {
		Debug(dInfo, "S%d receive stale RequestVote reply\n", rf.me)
		rf.rwMu.Unlock()
		return
	}
	// the reply server's Term > currentTerm
	if !reply.VoteGranted && reply.Term > rf.currentTerm {
		Debug(dLeader, "S%d turn into follower because receive reply of requestvote with high Term=%d\n", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.leaderId = -1
		rf.timeStart = time.Now()
	}
	rf.rwMu.Unlock()
	ch <- reply
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	Debug(dInfo, "S%d receive appendEntries currentTerm=%d\n", rf.me, rf.currentTerm)
	// 1. Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d refuse appendEntries because currentTerm=%d > Term=%d\n", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.timeStart = time.Now()
	// improve term
	if rf.currentTerm < args.Term {
		Debug(dInfo, "S%d turn into follower of S%d because currentTerm=%d < Term=%d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	// error have two leaders at the same time
	if rf.currentTerm == args.Term && rf.state == leader {
		Debug(dError, "S%d have two leader at the same term=%d\n", rf.me, rf.currentTerm)
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	if rf.lastLogIndex < args.PreLogIndex ||
		rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Success = false
		return
	}
	//	If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 	Append any new entries not already in the log
	for i := 1; i <= len(args.Entries); i++ {
		if args.PreLogIndex+i > rf.lastLogIndex ||
			rf.logs[args.PreLogIndex+i].Term != args.Entries[i-1].Term {
			Debug(dTrace, "S%d follower has append %d entries to its logs\n", rf.me, len(args.Entries)-i+1)
			rf.logs = rf.logs[:args.PreLogIndex+i]
			rf.lastLogIndex = args.PreLogIndex + len(args.Entries)
			rf.logs = append(rf.logs, args.Entries[i-1:]...)
			break
		}
	}
	//	If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAndHandleAppendEntries(server int, args *AppendEntriesArgs) {
	Debug(dLeader, "S%d send AppendEntries to S%d\n", rf.me, server)
	reply := AppendEntriesReply{}
	ok := false
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	}
	rf.rwMu.Lock()
	// receive stale reply
	if args.Term != rf.currentTerm {
		Debug(dInfo, "S%d receive stale appendentries reply\n", rf.me)
		rf.rwMu.Unlock()
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if !reply.Success && reply.Term > args.Term {
		if reply.Term > rf.currentTerm {
			Debug(dLeader, "S%d turn into follower because receive reply of appendentries with high Term=%d\n", rf.me, reply.Term)
			rf.state = follower
			rf.currentTerm = reply.Term
			rf.leaderId = -1
			rf.votedFor = -1
			rf.timeStart = time.Now()
			rf.rwMu.Unlock()
			return
		}
	}
	// someone has take over the task
	if rf.nextIndex[server] != args.PreLogIndex+1 {
		Debug(dLeader, "S%d other thread has take over the task\n", rf.me)
		rf.rwMu.Unlock()
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !reply.Success {
		Debug(dLeader, "S%d fail to send AppendEntries to S%d\n", rf.me, server)
		rf.nextIndex[server] = args.PreLogIndex
		args.PreLogIndex--
		args.PreLogTerm = rf.logs[args.PreLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		newEntries := make([]logEntry, len(rf.logs[args.PreLogIndex+1:]))
		copy(newEntries, rf.logs[args.PreLogIndex+1:])
		args.Entries = newEntries
		rf.rwMu.Unlock()
		go rf.sendAndHandleAppendEntries(server, args)
		return
	}
	// successful
	Debug(dLeader, "S%d succeed in sending AppendEntries to S%d\n", rf.me, server)
	rf.nextIndex[server] += len(args.Entries)
	rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
	rf.rwMu.Unlock()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	// if the server don't think he is a leader, return false
	if rf.state != leader {
		return -1, -1, false
	}
	Debug(dClient, "S%d Receive command\n", rf.me)
	// the server believe he is a leader
	term := rf.currentTerm
	rf.logs = append(rf.logs, logEntry{term, command})
	rf.lastLogIndex++
	index := rf.lastLogIndex
	// for i := 0; i < len(rf.peers); i++ {
	// 	// skip self
	// 	if i == rf.me {
	// 		continue
	// 	}
	// 	preIndex := rf.nextIndex[i] - 1
	// 	args := AppendEntriesArgs{rf.currentTerm, rf.me, preIndex, rf.logs[preIndex].Term, rf.logs[preIndex+1:], rf.commitIndex}
	// 	reply := AppendEntriesReply{}
	// 	go rf.sendAndHandleAppendEntries(i, &args, &reply)
	// }
	return index, term, true

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	killed := atomic.LoadInt32(&rf.dead)
	return killed == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.rwMu.RLock()
		timeBefore := time.Since(rf.timeStart)
		rf.rwMu.RUnlock()
		// get the randomize timeout and sleep
		electionTimeout := RaftElectionTimeoutAct/2 + time.Duration(rand.Int63n(int64(RaftElectionTimeoutAct/2)))
		time.Sleep(electionTimeout - timeBefore)

		// wake up and check whether timeout
		rf.rwMu.Lock()
		// disable the ticker if the server has become a leader
		if rf.state == leader {
			rf.rwMu.Unlock()
			continue
		}
		timeSince := time.Since(rf.timeStart)
		if timeSince > electionTimeout {
			Debug(dTimer, "S%d election timeout\n", rf.me)
			go rf.InitiateVote()
			rf.timeStart = time.Now()
		} else {
			Debug(dTimer, "S%d follower's normal check\n", rf.me)
		}
		rf.rwMu.Unlock()
	}
}

func (rf *Raft) heartbeats() {
	heartbeatsTimeout := time.Second / 10
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// sleep
		time.Sleep(heartbeatsTimeout)

		rf.rwMu.Lock()
		// if the leader lose its position, close heartbeats
		if rf.state != leader {
			rf.rwMu.Unlock()
			continue
		}
		Debug(dTimer, "S%d send heartbeats\n", rf.me)
		rf.timeStart = time.Now()
		// send heartbeat
		for i := 0; i < len(rf.peers); i++ {
			// skip self
			if i == rf.me {
				continue
			}
			preIndex := rf.nextIndex[i] - 1
			newEntries := make([]logEntry, len(rf.logs[preIndex+1:]))
			copy(newEntries, rf.logs[preIndex+1:])
			args := AppendEntriesArgs{rf.currentTerm, rf.me, preIndex, rf.logs[preIndex].Term, newEntries, rf.commitIndex}
			go rf.sendAndHandleAppendEntries(i, &args)
		}
		rf.rwMu.Unlock()
	}
}

func (rf *Raft) applyChecker(applych chan ApplyMsg) {
	applyTimeout := time.Second / 100
	for !rf.killed() {
		time.Sleep(applyTimeout)

		rf.rwMu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			Debug(dClient, "S%d apply logentry index=%d\n", rf.me, i)
			applyMsg := ApplyMsg{true, rf.logs[i].Command, i, false, nil, -1, -1}
			applych <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
		rf.rwMu.Unlock()
	}
}

func (rf *Raft) commitChecker() {
	commitTimeout := time.Millisecond * 10
	majority := rf.serversNum/2 + 1
	commitTmp := 0
	for !rf.killed() {
		time.Sleep(commitTimeout)

		rf.rwMu.Lock()
		if rf.state != leader {
			rf.rwMu.Unlock()
			continue
		}
		for i := commitTmp + 1; i <= rf.lastLogIndex; i++ {
			countRecord := 0
			for j := 0; j < rf.serversNum; j++ {
				if j == rf.me {
					countRecord++
				} else {
					if rf.matchIndex[j] >= i {
						countRecord++
					}
				}
			}
			if countRecord >= majority {
				commitTmp++
				if rf.logs[commitTmp].Term == rf.currentTerm {
					rf.commitIndex = commitTmp
					Debug(dCommit, "S%d commit entries to %d\n", rf.me, rf.commitIndex)
				}
			} else {
				break
			}
		}
		rf.rwMu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.serversNum = len(rf.peers)
	// Your initialization code here (2A, 2B, 2C).

	// Persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]logEntry, 1)
	rf.logs[0] = logEntry{0, nil}

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0

	// Volatile state on leaders
	rf.nextIndex = make([]int, rf.serversNum)
	rf.matchIndex = make([]int, rf.serversNum)

	rf.state = follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.timeStart = time.Now()
	go rf.ticker()
	go rf.heartbeats()
	go rf.applyChecker(applyCh)
	go rf.commitChecker()

	return rf
}

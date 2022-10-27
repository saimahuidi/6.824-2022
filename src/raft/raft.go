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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

	// persistent state on all server
	currentTerm int
	votedFor    int
	logs        []logEntry

	// for convenience
	baseLogIndex int
	lastLogIndex int

	// persist state which record the snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	// for keep persist before write
	currentTermTemp       int
	votedForTemp          int
	logsTemp              []logEntry
	lastIncludedIndexTemp int
	lastIncludedTermTemp  int

	// Volatile state on all server
	state       int
	leaderId    int
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// reinitialized after election
	nextIndex  []int
	matchIndex []int

	// to communicate with the client
	applych chan ApplyMsg
	// to inform append
	startch chan struct{}
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

func (rf *Raft) GetLeader() int {

	currentLeader := 0
	rf.rwMu.RLock()
	currentLeader = rf.leaderId
	rf.rwMu.RUnlock()
	return currentLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	Debug(dPersist, "S%d requests persist currentTerm=%d VoteFor=%d logIndex=%d\n", rf.me, rf.currentTermTemp, rf.votedForTemp, rf.lastLogIndex)
	rf.persister.SaveRaftState(rf.persistState())
}

func (rf *Raft) persistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTermTemp)
	e.Encode(rf.votedForTemp)
	e.Encode(rf.logsTemp)
	e.Encode(rf.lastIncludedTermTemp)
	e.Encode(rf.lastIncludedIndexTemp)
	return w.Bytes()
}

func (rf *Raft) getTerm(index int) int {
	if index > rf.lastLogIndex {
		Debug(dError, "S%d GetTerm error Index=%d lastLogIndex=%d\n", rf.me, index, rf.lastLogIndex)
		panic("index > rf.lastLogIndex")
	}
	if index >= rf.baseLogIndex {
		return rf.logs[index-rf.baseLogIndex].Term
	} else if index == rf.baseLogIndex-1 {
		return rf.lastIncludedTerm
	} else {
		Debug(dTerm, "S%d GetTerm error Index=%d baseLogIndex=%d\n", rf.me, index, rf.baseLogIndex)
		panic("index < rf.baseLogIndex - 1")
	}
}

func (rf *Raft) getCommand(index int) interface{} {
	if index > rf.lastLogIndex {
		Debug(dError, "S%d GetCommand error Index=%d lastLogIndex=%d\n", rf.me, index, rf.lastLogIndex)
		panic("index > rf.lastLogIndex")
	}
	if index >= rf.baseLogIndex {
		return rf.logs[index-rf.baseLogIndex].Command
	} else {
		Debug(dTerm, "S%d GetCommand error Index=%d baseLogIndex=%d\n", rf.me, index, rf.baseLogIndex)
		panic("index < rf.baseLogIndex - 1")
	}
}

func (rf *Raft) getLogs(index int) []logEntry {
	if index > rf.lastLogIndex+1 {
		Debug(dError, "S%d GetLogs error Index=%d lastLogIndex=%d\n", rf.me, index, rf.lastLogIndex)
		panic("index > rf.lastLogIndex")
	}
	if index >= rf.baseLogIndex {
		return rf.logs[index-rf.baseLogIndex:]
	} else {
		Debug(dTerm, "S%d GetLogs error Index=%d baseLogIndex=%d\n", rf.me, index, rf.baseLogIndex)
		panic("index < rf.baseLogIndex")
	}
}

func (rf *Raft) truncateLogs(index int) []logEntry {
	if index > rf.lastLogIndex+1 {
		Debug(dError, "S%d GetLogs error Index=%d lastLogIndex=%d\n", rf.me, index, rf.lastLogIndex)
		panic("index > rf.lastLogIndex")
	}
	if index >= rf.baseLogIndex {
		return rf.logs[:index-rf.baseLogIndex]
	} else {
		Debug(dTerm, "S%d GetLogs error Index=%d baseLogIndex=%d\n", rf.me, index, rf.baseLogIndex)
		panic("index < rf.baseLogIndex")
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []logEntry
	var lastIncludedTerm int
	var lastIncludedIndex int

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedTerm) != nil || d.Decode(&lastIncludedIndex) != nil {
		Debug(dError, "S%d readPersist fails\n")
	} else {
		// put in the temp
		rf.currentTermTemp = currentTerm
		rf.votedForTemp = voteFor
		rf.logsTemp = logs
		rf.lastIncludedTermTemp = lastIncludedTerm
		rf.lastIncludedIndexTemp = lastIncludedIndex

		// put in the raft state
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
		rf.logs = rf.logsTemp[:]
		rf.lastIncludedIndex = rf.lastIncludedIndexTemp
		rf.lastIncludedTerm = rf.lastIncludedTermTemp
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.baseLogIndex = lastIncludedIndex + 1
		rf.lastLogIndex = len(rf.logs) - 1 + rf.baseLogIndex
	}
	Debug(dPersist, "S%d readpersist result: currentTerm=%d VoteFor=%d baseIndex=%d lastIndex=%d\n", rf.me, rf.currentTerm, rf.votedFor, rf.baseLogIndex, rf.lastLogIndex)
}

func (rf *Raft) PassSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	var lastIncludedIndex int = rf.lastIncludedIndex
	var lastIncludedTerm int = rf.lastIncludedTerm
	Debug(dSnap, "S%d readSnapshot lastIncludedTerm=%d lastIncludedIndex=%d\n", rf.me, lastIncludedTerm, lastIncludedIndex)
	apply := ApplyMsg{false, nil, 0, true, snapshot, lastIncludedTerm, lastIncludedIndex}
	go func() {
		rf.applych <- apply
	}()
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
	rf.rwMu.Lock()
	Debug(dClient, "S%d request Snapshot before index=%d\n", rf.me, index)
	if rf.lastIncludedIndex > index {
		Debug(dClient, "S%d refuses Snapshot before index=%d because baseLogIndex = %d\n", rf.me, index, rf.baseLogIndex)
		rf.rwMu.Unlock()
		return
	}
	rf.lastIncludedIndexTemp = index
	rf.lastIncludedTermTemp = rf.getTerm(index)
	rf.logsTemp = rf.getLogs(index + 1)
	rf.baseLogIndex = index + 1
	Debug(dTrace, "S%d Snapshot remaining from %d to %d len=%d\n", rf.me, rf.baseLogIndex, rf.lastLogIndex, len(rf.logsTemp))
	persistState := rf.persistState()
	rf.persister.SaveStateAndSnapshot(persistState, snapshot)
	rf.lastIncludedIndex = rf.lastIncludedIndexTemp
	rf.lastIncludedTerm = rf.lastIncludedTermTemp
	rf.logs = rf.logsTemp[:]
	Debug(dClient, "S%d request Snapshot after baseLogIndex=%d\n", rf.me, rf.baseLogIndex)
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
	Debug(dVote, "S%d begin vote currentTerm=%d\n", rf.me, rf.currentTerm)
	rf.state = candidate
	rf.currentTermTemp++
	rf.votedForTemp = rf.me
	if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
		rf.persist()
	}
	rf.currentTerm = rf.currentTermTemp
	rf.votedFor = rf.votedForTemp
	// fill the args
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex, rf.getTerm(rf.lastLogIndex)}
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
	lastLogTerm := rf.getTerm(rf.lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastLogIndex)) {
		if args.Term > rf.currentTerm {
			Debug(dVote, "S%d agree to vote S%d and turn into follower because currentTerm=%d < Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			rf.state = follower
		} else {
			Debug(dVote, "S%d agree to vote S%d currentTerm=%d Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		}
		rf.currentTermTemp = args.Term
		rf.votedForTemp = args.CandidateId
		if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
			rf.persist()
		}
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
		reply.Term = rf.currentTerm
		rf.timeStart = time.Now()
		reply.VoteGranted = true
	} else {
		if args.Term > rf.currentTerm {
			Debug(dVote, "S%d rufuse to vote S%d but turn into follower because currentTerm=%d < Term=%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			rf.currentTermTemp = args.Term
			rf.votedForTemp = -1
			if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
				rf.persist()
			}
			rf.currentTerm = rf.currentTermTemp
			rf.votedFor = rf.votedForTemp
			rf.leaderId = -1
			rf.state = follower
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
		rf.currentTermTemp = reply.Term
		rf.votedForTemp = -1
		if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
			rf.persist()
		}
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
		rf.state = follower
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
		rf.currentTermTemp = args.Term
		rf.votedForTemp = -1
		if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
			rf.persist()
		}
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
		rf.state = follower
	}
	// error have two leaders at the same time
	if rf.currentTerm == args.Term && rf.state == leader {
		Debug(dError, "S%d have two leader at the same term=%d\n", rf.me, rf.currentTerm)
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm

	// if the PreLogIndex > than the baseIndex we need to skip log that has been delete
	advance := 0
	if args.PreLogIndex+1 < rf.baseLogIndex {
		advance = rf.baseLogIndex - args.PreLogIndex - 1
	}
	if args.PreLogIndex >= rf.baseLogIndex && (rf.lastLogIndex < args.PreLogIndex ||
		rf.getTerm(args.PreLogIndex) != args.PreLogTerm) {
		reply.Success = false
		Debug(dInfo, "S%d refuse the append because PreEntry isn't contained, PreLogIndex=%d PreLogTerm=%d\n", rf.me, args.PreLogIndex, args.PreLogTerm)
		return
	}
	//	If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 	Append any new entries not already in the log
	for i := 1 + advance; i <= len(args.Entries); i++ {
		if args.PreLogIndex+i > rf.lastLogIndex ||
			rf.getTerm(args.PreLogIndex+i) != args.Entries[i-1].Term {
			if args.PreLogIndex+i <= rf.lastLogIndex {
				rf.logsTemp = rf.truncateLogs(args.PreLogIndex + i)
			}
			rf.logsTemp = append(rf.logsTemp, args.Entries[i-1:]...)
			rf.lastLogIndex = args.PreLogIndex + len(args.Entries)
			rf.persist()
			rf.logs = rf.logsTemp[:]
			Debug(dTrace, "S%d follower has append %d entries to its logs\n", rf.me, len(args.Entries)-i+1)
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
	Debug(dLeader, "S%d send AppendEntries to S%d preIndex=%d len=%d\n", rf.me, server, args.PreLogIndex, len(args.Entries))
	reply := AppendEntriesReply{}
	ok := false
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	}
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	// receive stale reply
	if args.Term != rf.currentTerm {
		Debug(dInfo, "S%d receive stale appendentries reply from S%d\n", rf.me, server)
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if reply.Term > args.Term {
		if reply.Term > rf.currentTerm {
			Debug(dLeader, "S%d turn into follower because receive reply of appendentries with high Term=%d\n", rf.me, reply.Term)
			rf.currentTermTemp = reply.Term
			rf.votedForTemp = -1
			if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
				rf.persist()
			}
			rf.currentTerm = rf.currentTermTemp
			rf.votedFor = rf.votedForTemp
			rf.state = follower
			rf.leaderId = -1
			rf.timeStart = time.Now()
			return
		}
	}
	// someone has take over the task
	if rf.nextIndex[server] != args.PreLogIndex+1 {
		Debug(dLeader, "S%d other thread has take over the task\n", rf.me)
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !reply.Success {
		Debug(dLeader, "S%d fail to send AppendEntries to S%d\n", rf.me, server)
		// if we must have to use installSnapshot, we will
		if args.PreLogIndex <= rf.lastIncludedIndex {
			rf.nextIndex[server] = args.PreLogIndex
			go rf.sendAndHandleInstallSnapshot(server)
			return
		}

		preTerm := rf.getTerm(args.PreLogIndex)

		for args.PreLogIndex > rf.lastIncludedIndex && rf.getTerm(args.PreLogIndex) == preTerm {
			args.PreLogIndex--
		}
		rf.nextIndex[server] = args.PreLogIndex + 1
		args.PreLogTerm = rf.getTerm(args.PreLogIndex)
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		// if currently the server is installing snapshot, just send no entry
		newEntries := make([]logEntry, len(rf.getLogs(args.PreLogIndex+1)))
		copy(newEntries, rf.getLogs(args.PreLogIndex+1))
		args.Entries = newEntries
		go rf.sendAndHandleAppendEntries(server, args)
		return
	}
	// successful
	Debug(dLeader, "S%d succeed in sending AppendEntries to S%d index from %d to %d\n", rf.me, server, rf.nextIndex[server]-1, rf.nextIndex[server]+len(args.Entries)-1)
	rf.nextIndex[server] = args.PreLogIndex + len(args.Entries) + 1
	rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InitWithSnapshot(args *InstallSnapshotArgs) {
	var lastIncludedIndex int = args.LastIncludedIndex
	var lastIncludedTerm int = args.LastIncludedTerm
	var snapshot []byte = args.Data
	rf.lastIncludedIndexTemp = lastIncludedIndex
	rf.lastIncludedTermTemp = lastIncludedTerm
	Debug(dSnap, "S%d SnapshotIndex=%d SnapshotTerm=%d\n", rf.me, lastIncludedIndex, lastIncludedTerm)
	rf.baseLogIndex = lastIncludedIndex + 1
	rf.lastLogIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.logsTemp = make([]logEntry, 0)
	msg := ApplyMsg{false, nil, 0, true, snapshot, lastIncludedTerm, lastIncludedIndex}
	rf.persister.SaveStateAndSnapshot(rf.persistState(), snapshot)
	go func() {
		rf.applych <- msg
	}()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	Debug(dSnap, "S%d receive InstallSnapshot from S%d currentTerm=%d snapshotIndex=%d snapshotTerm=%d\n", rf.me, rf.leaderId, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	// if the RPC is stale
	if rf.currentTerm > args.Term {
		Debug(dSnap, "S%d rufuses the Snapshot because currentTerm=%d > Term=%d\n", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	}
	termChange := false
	logChange := false
	if args.Term > rf.currentTerm {
		Debug(dSnap, "S%d turns into follower because currentTerm=%d < Term=%d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTermTemp = args.Term
		rf.votedForTemp = -1
		rf.state = follower
		termChange = true
	}
	rf.leaderId = args.LeaderId
	// if the server has owned the content of the snapshot

	if rf.lastIncludedIndex >= args.LastIncludedIndex ||
		rf.lastLogIndex > args.LastIncludedIndex && rf.getTerm(args.LastIncludedIndex) == args.Term {
		Debug(dSnap, "S%d doesn't need to adopt the snapshot because contained already\n", rf.me)
	} else {
		logChange = true
		Debug(dSnap, "S%d adopts Snapshot from S%d currentTerm=%d\n", rf.me, rf.leaderId, rf.currentTerm)
		rf.InitWithSnapshot(args)
		rf.lastIncludedIndex = rf.lastIncludedIndexTemp
		rf.lastIncludedTerm = rf.lastIncludedTermTemp
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
		rf.logs = rf.logsTemp[:]
	}
	// save the Raft State if only the state changes
	if termChange && !logChange {
		Debug(dSnap, "S%d only the state needs to be stored\n", rf.me)
		rf.persist()
		rf.currentTerm = rf.currentTermTemp
		rf.votedFor = rf.votedForTemp
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAndHandleInstallSnapshot(server int) {
	Debug(dSnap, "S%d sends InstallSnapshot to S%d\n", rf.me, server)
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	ok := false
	for !ok {
		if rf.killed() {
			return
		}
		rf.rwMu.Lock()
		// if the server find it is not leader, abort the send
		if rf.state != leader {
			rf.rwMu.Unlock()
			return
		}
		if args.Term != rf.currentTerm || args.LastIncludedIndex != rf.lastIncludedIndex {
			args = InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
		}
		rf.rwMu.Unlock()
		ok = rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	}
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	// if receive stale reply
	if args.Term != rf.currentTerm {
		Debug(dInfo, "S%d receive stale InstallSnapshot reply from S%d\n", rf.me, server)
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if reply.Term > args.Term {
		if reply.Term > rf.currentTerm {
			Debug(dLeader, "S%d turn into follower because receive reply of InstallSnapshot with high Term=%d\n", rf.me, reply.Term)
			rf.currentTermTemp = reply.Term
			rf.votedForTemp = -1
			if rf.currentTerm != rf.currentTermTemp || rf.votedFor != rf.votedForTemp {
				rf.persist()
			}
			rf.currentTerm = rf.currentTermTemp
			rf.votedFor = rf.votedForTemp
			rf.state = follower
			rf.leaderId = -1
			rf.timeStart = time.Now()
			return
		}
	}
	// receive correct reply
	Debug(dSnap, "S%d sends InstallSnapshot to S%d successfully nextIndex=%d\n", rf.me, server, args.LastIncludedIndex+1)
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.rwMu.Lock()
	defer rf.rwMu.Unlock()
	// if the server don't think he is a leader, return false
	if rf.state != leader {
		return -1, -1, false
	}
	Debug(dClient, "S%d Receive command %v\n", rf.me, command)
	// the server believe he is a leader
	term := rf.currentTerm
	rf.logsTemp = append(rf.logsTemp, logEntry{term, command})
	rf.lastLogIndex++
	rf.persist()
	rf.logs = rf.logsTemp[:]
	index := rf.lastLogIndex
	// send the new start
	go func() {
		rf.startch <- struct{}{}
	}()
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
		electionTimeout := RaftElectionTimeoutAct/3 + time.Duration(rand.Int63n(int64(RaftElectionTimeoutAct/3*2)))
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
		select {
		case <-rf.startch:
			Debug(dLeader, "S%d send new start\n", rf.me)
		case <-time.After(heartbeatsTimeout):
		}

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
			currentIndex := rf.nextIndex[i]
			if currentIndex > rf.lastIncludedIndex {
				newEntries := make([]logEntry, len(rf.getLogs(currentIndex)))
				copy(newEntries, rf.getLogs(currentIndex))
				args := AppendEntriesArgs{rf.currentTerm, rf.me, preIndex, rf.getTerm(preIndex), newEntries, rf.commitIndex}
				go rf.sendAndHandleAppendEntries(i, &args)
			} else {
				go rf.sendAndHandleInstallSnapshot(i)
			}
		}
		rf.rwMu.Unlock()
	}
}

func (rf *Raft) applyChecker() {
	applyTimeout := time.Second / 100
	for !rf.killed() {
		time.Sleep(applyTimeout)

		rf.rwMu.Lock()
		if rf.lastApplied < rf.commitIndex {
			applyMsgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsgs = append(applyMsgs, ApplyMsg{true, rf.getCommand(i), i, false, nil, -1, -1})
			}
			Debug(dClient, "S%d apply logEntry from index=%d to index=%d\n", rf.me, rf.lastApplied+1, rf.commitIndex)
			rf.lastApplied = rf.commitIndex
			rf.rwMu.Unlock()
			for i := 0; i < len(applyMsgs); i++ {
				Debug(dClient, "S%d is applying command %v\n", rf.me, applyMsgs[i].Command)
				rf.applych <- applyMsgs[i]
			}
		} else {
			rf.rwMu.Unlock()
		}
	}
}

func (rf *Raft) commitChecker() {
	commitTimeout := time.Millisecond * 10
	majority := rf.serversNum/2 + 1
	for !rf.killed() {
		time.Sleep(commitTimeout)

		rf.rwMu.Lock()
		if rf.state != leader {
			rf.rwMu.Unlock()
			continue
		}
		// in case that the commitIndex has changed because snapshotRead
		for i := rf.commitIndex + 1; i <= rf.lastLogIndex; i++ {
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
				if rf.getTerm(i) == rf.currentTerm {
					rf.commitIndex = i
					Debug(dCommit, "S%d commit to entries %d\n", rf.me, rf.commitIndex)
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
	rf.applych = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// Persistent state on all servers
	rf.currentTermTemp = 0
	rf.currentTerm = 0
	rf.votedForTemp = -1
	rf.votedFor = -1
	rf.baseLogIndex = 1
	rf.lastLogIndex = 0
	rf.logsTemp = make([]logEntry, 0)
	rf.logs = rf.logsTemp[:]

	rf.lastIncludedIndexTemp = 0
	rf.lastIncludedIndexTemp = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders
	rf.nextIndex = make([]int, rf.serversNum)
	rf.matchIndex = make([]int, rf.serversNum)
	rf.startch = make(chan struct{})

	rf.state = follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.PassSnapshot(rf.persister.ReadSnapshot())
	// start ticker goroutine to start elections
	rf.timeStart = time.Now()
	go rf.ticker()
	go rf.heartbeats()
	go rf.applyChecker()
	go rf.commitChecker()

	return rf
}

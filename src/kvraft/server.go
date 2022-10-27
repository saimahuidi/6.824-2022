package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientId  int32
	CommandID int32
	Operation string
	Key       string
	Value     string
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	currentIndex   int
	maxraftstate   int // snapshot if log grows this big
	receiveLogSize int
	persister      *raft.Persister

	// the kv database
	dataBaseMu sync.Mutex
	dataBase   map[string]string

	// the info of each client
	mapMu             sync.RWMutex
	clientInfoMap     map[int32]*clientInfo
	clientInfoLockMap map[int32]*clientInfoLock

	// to control snapshotcheck
	snapshotMu sync.Mutex
}

type clientInfo struct {
	NextCommandId int32
	LastReply     string
}

type clientInfoLock struct {
	RwMu sync.RWMutex
	Cond sync.Cond
}

type ReadMapCondition struct {
	rwMu *sync.RWMutex
}

func (rM *ReadMapCondition) Lock() {
	rM.rwMu.RLock()
}

func (rM *ReadMapCondition) Unlock() {
	rM.rwMu.RUnlock()
}

func (kv *KVServer) GetClientInfo(clientId int32) (*clientInfo, *clientInfoLock) {
	// if the client first comes
	kv.mapMu.RLock()
	thisClientInfo, ok1 := kv.clientInfoMap[clientId]
	thisClientInfoLock, ok2 := kv.clientInfoLockMap[clientId]
	kv.mapMu.RUnlock()
	if !ok1 || !ok2 {
		kv.mapMu.Lock()
		thisClientInfo, ok1 = kv.clientInfoMap[clientId]
		thisClientInfoLock, ok2 = kv.clientInfoLockMap[clientId]
		if !ok1 {
			thisClientInfo = new(clientInfo)
			thisClientInfo.NextCommandId = 0
			kv.clientInfoMap[clientId] = thisClientInfo
		}
		if !ok2 {
			thisClientInfoLock = new(clientInfoLock)
			thisClientInfoLock.Cond.L = &ReadMapCondition{&thisClientInfoLock.RwMu}
			kv.clientInfoLockMap[clientId] = thisClientInfoLock
		}
		kv.mapMu.Unlock()
	}
	return thisClientInfo, thisClientInfoLock
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// return if the server is not a leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DeBug(dLeader, "S%d receive Get ClientId = %d CommandId = %d Key = %s\n", kv.me, args.ClientId, args.CommandID, args.Key)
	clientId := args.ClientId
	commandId := args.CommandID
	clientInfoT, clientInfoLockT := kv.GetClientInfo(clientId)
	clientInfoLockT.RwMu.RLock()
	nextCommand := clientInfoT.NextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		reply.Value = clientInfoT.LastReply
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{clientId, commandId, "Get", args.Key, ""}
	kv.rf.Start(operation)
	DeBug(dLeader, "S%d start %v\n", kv.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !kv.killed() {
		if commandId < clientInfoT.NextCommandId {
			if commandId == clientInfoT.NextCommandId-1 {
				reply.Err = OK
				reply.Value = clientInfoT.LastReply
			} else {
				reply.Err = ErrRepeat
			}
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// return if the server is not a leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DeBug(dLeader, "S%d receive %s ClientId = %d CommandId = %d Key = %s value = %s\n", kv.me, args.Op, args.ClientId, args.CommandID, args.Key, args.Value)
	clientId := args.ClientId
	commandId := args.CommandID
	clientInfoT, clientInfoLockT := kv.GetClientInfo(clientId)
	clientInfoLockT.RwMu.RLock()
	nextCommand := clientInfoT.NextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{clientId, commandId, args.Op, args.Key, args.Value}
	kv.rf.Start(operation)
	DeBug(dLeader, "S%d start %v\n", kv.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !kv.killed() {
		if commandId < clientInfoT.NextCommandId {
			if commandId == clientInfoT.NextCommandId-1 {
				reply.Err = OK
			} else {
				reply.Err = ErrRepeat
			}
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (kv *KVServer) checkSnapshot() {
	// if there is no need to snapshot
	if kv.maxraftstate == -1 || kv.receiveLogSize < kv.maxraftstate {
		return
	}
	kv.receiveLogSize = 0
	kv.mapMu.RLock()
	kv.snapshotMu.Lock()
	defer kv.mapMu.RUnlock()
	defer kv.snapshotMu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dataBase)
	e.Encode(kv.clientInfoMap)
	kv.rf.Snapshot(kv.currentIndex, w.Bytes())
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyCheaker() {
	for {
		receiveMessage := <-kv.applyCh
		if kv.killed() {
			return
		}
		if receiveMessage.CommandValid {
			// DeBug(dInfo, "S%d receive apply log %v\n", kv.me, receiveMessage.Command)
			kv.handleApplyLog(&receiveMessage)
			kv.receiveLogSize += int(unsafe.Sizeof(receiveMessage))
		} else if receiveMessage.SnapshotValid {
			kv.handleSnapshot(&receiveMessage)
		} else {
			panic("should not reach here\n")
		}
	}
}

func (kv *KVServer) handleApplyLog(receiveMessage *raft.ApplyMsg) {
	if receiveMessage.CommandIndex != kv.currentIndex+1 {
		return
	}
	var operation Op = receiveMessage.Command.(Op)
	// get the content
	clientId := operation.ClientId
	commandId := operation.CommandID
	key := operation.Key
	op := operation.Operation
	value := operation.Value
	// DeBug(dInfo, "S%d receive apply clientId = %d commandId = %d key = %s op = %s value = %s\n", kv.me, clientId, commandId, key, op, value)
	clientInfoT, clientInfoLockT := kv.GetClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	kv.dataBaseMu.Lock()
	defer clientInfoLockT.RwMu.Unlock()
	defer kv.dataBaseMu.Unlock()
	// increase the index of log
	kv.currentIndex++
	// check whether the id match to avoid twice excute
	if clientInfoT.NextCommandId != commandId {
		kv.checkSnapshot()
		clientInfoLockT.Cond.Broadcast()
		return
	}
	// commit the op to the state machine
	switch op {
	case GetOp:
		value = kv.dataBase[key]
		clientInfoT.LastReply = value
	case PutOp:
		kv.dataBase[key] = value
	case AppendOp:
		preValue := kv.dataBase[key]
		kv.dataBase[key] = preValue + value
	}
	clientInfoT.NextCommandId++
	kv.checkSnapshot()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
}

func (kv *KVServer) handleSnapshot(receiveMessage *raft.ApplyMsg) {
	kv.dataBaseMu.Lock()
	kv.mapMu.Lock()
	kv.snapshotMu.Lock()
	defer kv.dataBaseMu.Unlock()
	defer kv.mapMu.Unlock()
	defer kv.snapshotMu.Unlock()
	r := bytes.NewBuffer(receiveMessage.Snapshot)
	d := labgob.NewDecoder(r)
	var databaseTemp map[string]string
	var clientInfoMapTemp map[int32]*clientInfo
	if d.Decode(&databaseTemp) != nil || d.Decode(&clientInfoMapTemp) != nil {
		panic("Read Snapshot fails\n")
	}
	kv.currentIndex = receiveMessage.SnapshotIndex
	kv.dataBase = databaseTemp
	kv.clientInfoMap = clientInfoMapTemp
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dataBase = make(map[string]string)
	kv.clientInfoMap = make(map[int32]*clientInfo)
	kv.clientInfoLockMap = make(map[int32]*clientInfoLock)
	// You may need initialization code here.
	go kv.applyCheaker()
	return kv
}

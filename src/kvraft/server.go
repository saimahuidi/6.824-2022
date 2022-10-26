package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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

	currentIndex int
	maxraftstate int // snapshot if log grows this big

	// the kv database
	dataMu   sync.RWMutex
	dataBase map[string]string

	// the info of each client
	mapMu         sync.RWMutex
	clientInfoMap map[int32]*clientInfo
}

type clientInfo struct {
	rwMu              sync.RWMutex
	cond              sync.Cond
	nextCommandId     int32
	nextCommandSubmit bool
	lastReply         string
}

func (kv *KVServer) GetClientInfo(clientId int32) *clientInfo {
	// if the client first comes
	kv.mapMu.RLock()
	thisClientInfo, ok := kv.clientInfoMap[clientId]
	kv.mapMu.RUnlock()
	if !ok {
		kv.mapMu.Lock()
		thisClientInfo, ok = kv.clientInfoMap[clientId]
		if !ok {
			thisClientInfo = new(clientInfo)
			thisClientInfo.nextCommandId = 0
			thisClientInfo.cond.L = &sync.Mutex{}
			kv.clientInfoMap[clientId] = thisClientInfo
		}
		kv.mapMu.Unlock()
	}
	return thisClientInfo
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
	clientInfoT := kv.GetClientInfo(clientId)
	clientInfoT.rwMu.Lock()
	nextCommand := clientInfoT.nextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		reply.Value = clientInfoT.lastReply
		clientInfoT.rwMu.Unlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoT.rwMu.Unlock()
		return
	}
	if !clientInfoT.nextCommandSubmit {
		operation := Op{clientId, commandId, "Get", args.Key, ""}
		kv.rf.Start(operation)
		clientInfoT.nextCommandSubmit = true
	}
	clientInfoT.rwMu.Unlock()
	// wait for the apply
	clientInfoT.cond.L.Lock()
	defer clientInfoT.cond.L.Unlock()
	for !kv.killed() {
		clientInfoT.rwMu.RLock()
		if commandId < clientInfoT.nextCommandId {
			if commandId == clientInfoT.nextCommandId-1 {
				reply.Err = OK
				reply.Value = clientInfoT.lastReply
			} else {
				reply.Err = ErrRepeat
			}
			clientInfoT.rwMu.RUnlock()
			return
		}
		clientInfoT.rwMu.RUnlock()
		clientInfoT.cond.Wait()
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
	clientInfoT := kv.GetClientInfo(clientId)
	clientInfoT.rwMu.Lock()
	nextCommand := clientInfoT.nextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		clientInfoT.rwMu.Unlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoT.rwMu.Unlock()
		return
	}
	if !clientInfoT.nextCommandSubmit {
		operation := Op{clientId, commandId, args.Op, args.Key, args.Value}
		kv.rf.Start(operation)
		clientInfoT.nextCommandSubmit = true
	}
	clientInfoT.rwMu.Unlock()
	// wait for the apply
	clientInfoT.cond.L.Lock()
	defer clientInfoT.cond.L.Unlock()
	for !kv.killed() {
		clientInfoT.rwMu.RLock()
		if commandId < clientInfoT.nextCommandId {
			if commandId == clientInfoT.nextCommandId-1 {
				reply.Err = OK
			} else {
				reply.Err = ErrRepeat
			}
			clientInfoT.rwMu.RUnlock()
			return
		}
		clientInfoT.rwMu.RUnlock()
		clientInfoT.cond.Wait()
	}
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
			// DeBug(dInfo, "S%d receive apply log %d\n", kv.me, receiveMessage.CommandIndex)
			kv.handleApplyLog(&receiveMessage)
		} else if receiveMessage.SnapshotValid {
			kv.handleSnapshot(&receiveMessage)
		} else {
			panic("should not reach here\n")
		}
	}
}

func (kv *KVServer) handleApplyLog(receiveMessage *raft.ApplyMsg) {
	if receiveMessage.CommandIndex != kv.currentIndex+1 {
		panic("the order of apply index is wrong!\n")
	}
	kv.currentIndex = receiveMessage.CommandIndex
	var operation Op = receiveMessage.Command.(Op)
	// get the content
	clientId := operation.ClientId
	commandId := operation.CommandID
	key := operation.Key
	op := operation.Operation
	value := operation.Value
	// DeBug(dInfo, "S%d receive apply clientId = %d commandId = %d key = %s op = %s value = %s\n", kv.me, clientId, commandId, key, op, value)
	clientInfoT := kv.GetClientInfo(clientId)
	clientInfoT.rwMu.Lock()
	kv.dataMu.Lock()
	defer clientInfoT.rwMu.Unlock()
	defer kv.dataMu.Unlock()

	// check the whether the id match to avoid twice excute
	if clientInfoT.nextCommandId != commandId {
		panic("the order of apply index is wrong\n")
	}
	// commit the op to the state machine
	switch op {
	case GetOp:
		value = kv.dataBase[key]
		clientInfoT.lastReply = value
	case PutOp:
		kv.dataBase[key] = value
	case AppendOp:
		preValue := kv.dataBase[key]
		kv.dataBase[key] = preValue + value
	}
	clientInfoT.nextCommandId++
	clientInfoT.nextCommandSubmit = false
	// inform the waited RPC
	clientInfoT.cond.Broadcast()
}

func (kv *KVServer) handleSnapshot(receiveMessage *raft.ApplyMsg) {

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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dataBase = make(map[string]string)
	kv.clientInfoMap = make(map[int32]*clientInfo)
	// You may need initialization code here.
	go kv.applyCheaker()
	return kv
}

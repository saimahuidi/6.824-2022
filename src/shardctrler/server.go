package shardctrler

import (
	"bytes"
	"container/list"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	currentIndex   int // to record the recentIndex received
	maxraftstate   int // snapshot if log grows this big
	receiveLogSize int // accumulate to record the current size
	persister      *raft.Persister
	// the configs stored
	configs         []Config // indexed by config num
	aliveGroupsList list.List
	groupLocator    map[int]*list.Element
	// the next Id to design
	waitIdCond   sync.Cond
	waitIdLock   ReadMapCondition
	waitList     []*GetClientIdReply
	nextClientId int32
	// the info of each client
	mapMu             sync.RWMutex
	clientInfoMap     map[int32]*clientInfo
	clientInfoLockMap map[int32]*clientInfoLock
	// to control snapshotcheck
	snapshotMu sync.Mutex
}

// used to pass to the raft
type Op struct {
	OperationId int
	Args        interface{}
}

// used to distribute clientId
type GetIdOp struct {
	ServerId int
	Term     int
}

// used to store the last reply to avoid double excute
type clientInfo struct {
	NextCommandId int32
	LastReply     interface{}
}

// used to seriablize every client's command
type clientInfoLock struct {
	RwMu sync.RWMutex
	Cond sync.Cond
}

type groupInfo struct {
	GID    int
	shards []int
}

type ReadMapCondition struct {
	rwMu *sync.RWMutex
}

// to make a read lock for cond
func (rM *ReadMapCondition) Lock() {
	rM.rwMu.RLock()
}

func (rM *ReadMapCondition) Unlock() {
	rM.rwMu.RUnlock()
}

func (sc *ShardCtrler) getClientInfo(clientId int32) (*clientInfo, *clientInfoLock) {
	// if the client first comes
	sc.mapMu.RLock()
	thisClientInfo, ok1 := sc.clientInfoMap[clientId]
	thisClientInfoLock, ok2 := sc.clientInfoLockMap[clientId]
	sc.mapMu.RUnlock()
	if !ok1 || !ok2 {
		sc.mapMu.Lock()
		thisClientInfo, ok1 = sc.clientInfoMap[clientId]
		thisClientInfoLock, ok2 = sc.clientInfoLockMap[clientId]
		if !ok1 {
			thisClientInfo = new(clientInfo)
			thisClientInfo.NextCommandId = 0
			sc.clientInfoMap[clientId] = thisClientInfo
		}
		if !ok2 {
			thisClientInfoLock = new(clientInfoLock)
			thisClientInfoLock.Cond.L = &ReadMapCondition{&thisClientInfoLock.RwMu}
			sc.clientInfoLockMap[clientId] = thisClientInfoLock
		}
		sc.mapMu.Unlock()
	}
	return thisClientInfo, thisClientInfoLock
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// return if the server is not a leader
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dLeader, "S%d receive Join ClientId = %d CommandId = %d servers = %v\n", sc.me, args.ClientId, args.CommandId, args.Servers)
	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
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
		reply.Err = OK
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{Join, *args}
	sc.rf.Start(operation)
	Debug(dLeader, "S%d start %v\n", sc.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !sc.killed() {
		if commandId < clientInfoT.NextCommandId {
			reply.Err = OK
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// return if the server is not a leader
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dLeader, "S%d receive Leave ClientId = %d CommandId = %d GIDs = %v\n", sc.me, args.ClientId, args.CommandId, args.GIDs)
	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
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
		reply.Err = OK
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{Leave, *args}
	sc.rf.Start(operation)
	Debug(dLeader, "S%d start %v\n", sc.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !sc.killed() {
		if commandId < clientInfoT.NextCommandId {
			reply.Err = OK
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// return if the server is not a leader
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dLeader, "S%d receive Move ClientId = %d CommandId = %d GIDs = %v shard = %v\n", sc.me, args.ClientId, args.CommandId, args.GID, args.Shard)
	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
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
		reply.Err = OK
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{Move, *args}
	sc.rf.Start(operation)
	Debug(dLeader, "S%d start %v\n", sc.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !sc.killed() {
		if commandId < clientInfoT.NextCommandId {
			reply.Err = OK
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// return if the server is not a leader
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dLeader, "S%d receive Query ClientId = %d CommandId = %d num = %d\n", sc.me, args.ClientId, args.CommandId, args.Num)
	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
	clientInfoLockT.RwMu.RLock()
	nextCommand := clientInfoT.NextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		reply.Config = clientInfoT.LastReply.(Config)
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = OK
		clientInfoLockT.RwMu.RUnlock()
		return
	}
	operation := Op{Query, *args}
	sc.rf.Start(operation)
	Debug(dLeader, "S%d start %v\n", sc.me, operation)
	clientInfoLockT.RwMu.RUnlock()
	// wait for the apply
	clientInfoLockT.Cond.L.Lock()
	defer clientInfoLockT.Cond.L.Unlock()
	for !sc.killed() {
		if commandId < clientInfoT.NextCommandId {
			if commandId == clientInfoT.NextCommandId-1 {
				reply.Config = clientInfoT.LastReply.(Config)
			}
			reply.Err = OK
			return
		}
		clientInfoLockT.Cond.Wait()
	}
}

func (sc *ShardCtrler) GetClientId(args *GetClientIdArgs, reply *GetClientIdReply) {
	// return if the server is not a leader
	curTerm, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dLeader, "S%d receive GetClientId\n", sc.me)
	sc.waitIdLock.rwMu.Lock()
	reply.Err = WaitForClientId
	sc.waitList = append(sc.waitList, reply)
	sc.waitIdLock.rwMu.Unlock()
	operation := Op{GetClientId, GetIdOp{sc.me, curTerm}}
	sc.rf.Start(operation)
	// wait for reply
	sc.waitIdCond.L.Lock()
	defer sc.waitIdCond.L.Unlock()
	for !sc.killed() {
		switch reply.Err {
		case OK:
			return
		case ErrWrongLeader:
			return
		}
		sc.waitIdCond.Wait()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(GetIdOp{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.waitList = make([]*GetClientIdReply, 0)
	sc.waitIdLock.rwMu = new(sync.RWMutex)
	sc.waitIdCond.L = &sc.waitIdLock
	sc.maxraftstate = -1
	sc.nextClientId = 0

	sc.clientInfoLockMap = make(map[int32]*clientInfoLock)
	sc.clientInfoMap = make(map[int32]*clientInfo)

	sc.aliveGroupsList.Init()
	sc.groupLocator = make(map[int]*list.Element)

	sc.currentIndex = 0

	go sc.applyCheaker()
	return sc
}

func (sc *ShardCtrler) applyCheaker() {
	for !sc.killed() {
		receiveMessage := <-sc.applyCh
		if receiveMessage.CommandValid && !receiveMessage.SnapshotValid {
			// DeBug(dInfo, "S%d receive apply log %v\n", sc.me, receiveMessage.Command)
			sc.handleApplyLog(&receiveMessage)
			sc.receiveLogSize += int(unsafe.Sizeof(receiveMessage))
		} else if receiveMessage.SnapshotValid && !receiveMessage.CommandValid {
			sc.handleSnapshot(&receiveMessage)
		} else {
			panic("apply has mistake\n")
		}
	}
}

func (sc *ShardCtrler) handleApplyLog(receiveMessage *raft.ApplyMsg) {
	// if the CommandIndex not match, when receive stale apply after snapshot
	if receiveMessage.CommandIndex != sc.currentIndex+1 {
		return
	}
	sc.currentIndex++
	var operation Op = receiveMessage.Command.(Op)
	switch operation.OperationId {
	case GetClientId:
		sc.getClientIdHandler(&operation)
	case Query:
		sc.queryHandler(&operation)
	case Join:
		sc.joinHandler(&operation)
	case Leave:
		sc.leaveHandler(&operation)
	case Move:
		sc.moveHandler(&operation)
	}
	// if _, isLeader := sc.rf.GetState(); isLeader {
	// 	log.Printf("%v\n", sc.configs[len(sc.configs)-1].Groups)
	// }
}

func (sc *ShardCtrler) handleSnapshot(receiveMessage *raft.ApplyMsg) {

}

func (sc *ShardCtrler) checkSnapshot() {
	// if there is no need to snapshot
	if sc.maxraftstate == -1 || sc.receiveLogSize < sc.maxraftstate {
		return
	}
	sc.receiveLogSize = 0
	sc.mapMu.RLock()
	sc.snapshotMu.Lock()
	defer sc.mapMu.RUnlock()
	defer sc.snapshotMu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.clientInfoMap)
	sc.rf.Snapshot(sc.currentIndex, w.Bytes())
}

func (sc *ShardCtrler) getClientIdHandler(operation *Op) {
	curTerm, isLeader := sc.rf.GetState()
	args := operation.Args.(GetIdOp)
	sc.waitIdLock.rwMu.Lock()
	if args.ServerId == sc.me && args.Term == curTerm {
		if len(sc.waitList) > 0 {
			sc.waitList[0].Err = OK
			sc.waitList[0].ClientId = sc.nextClientId
			sc.waitList = sc.waitList[1:]
		}
	} else {
		if !isLeader {
			for _, gcir := range sc.waitList {
				gcir.Err = ErrWrongLeader
			}
			sc.waitList = sc.waitList[:0]
		}
	}
	sc.nextClientId++
	sc.waitIdLock.rwMu.Unlock()
	sc.waitIdCond.Broadcast()
}

func (sc *ShardCtrler) queryHandler(operation *Op) {
	args := operation.Args.(QueryArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	num := args.Num
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	defer clientInfoLockT.RwMu.Unlock()
	// check whether the id match to avoid twice excute
	if clientInfoT.NextCommandId != commandId {
		sc.checkSnapshot()
		clientInfoLockT.Cond.Broadcast()
		return
	}
	clientInfoT.NextCommandId++
	// do corresponding operation
	if num < 0 || num >= len(sc.configs) {
		clientInfoT.LastReply = sc.configs[len(sc.configs)-1]
	} else {
		clientInfoT.LastReply = sc.configs[num]
	}
	sc.checkSnapshot()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
}

func (sc *ShardCtrler) joinHandler(operation *Op) {
	args := operation.Args.(JoinArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	servers := args.Servers
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	defer clientInfoLockT.RwMu.Unlock()
	// check whether the id match to avoid twice excute
	if clientInfoT.NextCommandId != commandId {
		sc.checkSnapshot()
		clientInfoLockT.Cond.Broadcast()
		return
	}
	clientInfoT.NextCommandId++
	// do corresponding operation
	curConfig := sc.createNewConfig()
	// create the freeShardsList
	var freeShardsList []int
	if sc.aliveGroupsList.Len() != 0 {
		freeShardsList = make([]int, 0)
	} else {
		freeShardsList = make([]int, NShards)
		for i := 0; i < NShards; i++ {
			freeShardsList[i] = i
		}
	}
	newGroups := make([]int, 0, len(servers))
	// join the new group
	for i, v := range servers {
		if _, ok := curConfig.Groups[i]; ok {
			panic("join repeat group id\n")
		}
		curConfig.Groups[i] = v
		newGroups = append(newGroups, i)
	}
	// to be deterministc, sort the alivegroups
	sort.Slice(newGroups, func(i, j int) bool {
		return newGroups[i] < newGroups[j]
	})
	// insert the new server to the aliveGroups
	for _, v := range newGroups {
		sc.groupLocator[v] = sc.aliveGroupsList.PushBack(groupInfo{v, make([]int, 0)})
	}
	sc.loadBalance(freeShardsList)
	// operation over
	sc.checkSnapshot()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
}

func (sc *ShardCtrler) moveHandler(operation *Op) {
	args := operation.Args.(MoveArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	GID := args.GID
	shard := args.Shard
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	defer clientInfoLockT.RwMu.Unlock()
	// check whether the id match to avoid twice excute
	if clientInfoT.NextCommandId != commandId {
		sc.checkSnapshot()
		clientInfoLockT.Cond.Broadcast()
		return
	}
	clientInfoT.NextCommandId++
	// do corresponding operation
	curConfig := sc.createNewConfig()
	oldGID := curConfig.Shards[shard]
	curConfig.Shards[shard] = GID
	sc.redistributShard(GID, oldGID, shard)
	// over
	sc.checkSnapshot()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
}

func (sc *ShardCtrler) leaveHandler(operation *Op) {
	args := operation.Args.(LeaveArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	GIDs := args.GIDs
	clientInfoT, clientInfoLockT := sc.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	defer clientInfoLockT.RwMu.Unlock()
	// check whether the id match to avoid twice excute
	if clientInfoT.NextCommandId != commandId {
		sc.checkSnapshot()
		clientInfoLockT.Cond.Broadcast()
		return
	}
	clientInfoT.NextCommandId++
	// do corresponding operation
	curConfig := sc.createNewConfig()
	// record the group to delete
	removeSet := make(map[int]struct{})
	// delete the old group
	for _, v := range GIDs {
		if _, ok := curConfig.Groups[v]; !ok {
			panic("remove not exist group id\n")
		}
		delete(curConfig.Groups, v)
		removeSet[v] = struct{}{}
	}
	if len(curConfig.Groups) == 0 {
		sc.aliveGroupsList.Init()
		for i := 0; i < NShards; i++ {
			curConfig.Shards[i] = 0
		}
	} else {
		freeShardsList := sc.removeAndGetFreeShards(removeSet)
		sc.loadBalance(freeShardsList)
	}
	sc.checkSnapshot()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
}

func (sc *ShardCtrler) loadBalance(freeShardsList []int) {
	curConfig := &sc.configs[len(sc.configs)-1]
	groupNum := sc.aliveGroupsList.Len()
	perServerLoad := NShards / groupNum
	remnant := NShards % groupNum
	curGroupElem := sc.aliveGroupsList.Front()
	for i := 0; i < groupNum; i++ {
		curGroupId := curGroupElem.Value.(groupInfo).GID
		curGroupshards := curGroupElem.Value.(groupInfo).shards
		thisServerLoad := perServerLoad
		if remnant > 0 {
			thisServerLoad++
			remnant--
		}
		thisShardsLen := len(curGroupshards)
		if thisShardsLen > thisServerLoad {
			freeShardsList = append(freeShardsList, curGroupshards[thisServerLoad:]...)
			curGroupElem.Value = groupInfo{curGroupId, curGroupshards[:thisServerLoad]}
		} else if thisShardsLen < thisServerLoad {
			new_distribute_shard := freeShardsList[:thisServerLoad-thisShardsLen]
			freeShardsList = freeShardsList[thisServerLoad-thisShardsLen:]
			curGroupElem.Value = groupInfo{curGroupId, append(curGroupshards, new_distribute_shard...)}
			for _, v := range new_distribute_shard {
				curConfig.Shards[v] = curGroupId
			}
		}
		curGroupElem = curGroupElem.Next()
	}
	if len(freeShardsList) > 0 {
		panic("distribute shards fails\n")
	}
}

func (sc *ShardCtrler) removeAndGetFreeShards(removeSet map[int]struct{}) []int {
	freeShardsList := make([]int, 0)
	for i := range removeSet {
		deleteElem := sc.groupLocator[i]
		delete(sc.groupLocator, i)
		freeShardsList = append(freeShardsList, deleteElem.Value.(groupInfo).shards...)
		sc.aliveGroupsList.Remove(deleteElem)
	}
	sort.Slice(freeShardsList, func(i, j int) bool {
		return freeShardsList[i] < freeShardsList[j]
	})
	return freeShardsList
}

func (sc *ShardCtrler) createNewConfig() *Config {
	lastConfig := &sc.configs[len(sc.configs)-1]
	sc.configs = append(sc.configs, Config{lastConfig.Num + 1, lastConfig.Shards, make(map[int][]string)})
	curConfig := &sc.configs[len(sc.configs)-1]
	// copy the orign groups
	for i, v := range lastConfig.Groups {
		curConfig.Groups[i] = v
	}
	return curConfig
}

func (sc *ShardCtrler) redistributShard(newGID, oldGID int, shard int) {
	if newGID == oldGID {
		return
	}
	// append the shard to the new group
	newGroupElem := sc.groupLocator[newGID]
	newGroupId := newGroupElem.Value.(groupInfo).GID
	newGroupshards := newGroupElem.Value.(groupInfo).shards
	newGroupshards = append(newGroupshards, shard)
	newGroupElem.Value = groupInfo{newGroupId, newGroupshards}
	sc.aliveGroupsList.Remove(newGroupElem)
	// remove the shard from the old group
	oldGroupElem := sc.groupLocator[oldGID]
	oldGroupId := newGroupElem.Value.(groupInfo).GID
	oldGroupshards := newGroupElem.Value.(groupInfo).shards
	for i, v := range oldGroupshards {
		if v == shard && i != len(oldGroupshards)-1 {
			oldGroupshards = append(oldGroupshards[:i], oldGroupshards[i+1:]...)
			break
		} else if v == shard {
			oldGroupshards = oldGroupshards[:i]
			break
		}
	}
	oldGroupElem.Value = groupInfo{oldGroupId, oldGroupshards}
	sc.aliveGroupsList.Remove(oldGroupElem)
	// reinsert the two group
	curGroupElem := sc.aliveGroupsList.Front()
	var old_flag, new_flag bool
	for i := 0; i < sc.aliveGroupsList.Len(); i++ {
		nextGroupElem := curGroupElem.Next()
		curGroupshards := curGroupElem.Value.(groupInfo).shards
		if len(oldGroupshards) >= len(curGroupshards) {
			sc.aliveGroupsList.MoveBefore(oldGroupElem, curGroupElem)
			old_flag = true
		}
		if len(newGroupshards) >= len(curGroupshards) {
			sc.aliveGroupsList.MoveBefore(nextGroupElem, curGroupElem)
			new_flag = true
		}
		if new_flag && old_flag {
			break
		}
		curGroupElem = nextGroupElem
	}
}

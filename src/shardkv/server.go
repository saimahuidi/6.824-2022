package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	dead     int32

	// used to get config from shardctler
	ctrlerClient *shardctrler.Clerk

	// used to record log
	currentIndex int

	// used to snapshot
	maxraftstate   int // snapshot if log grows this big
	receiveLogSize int
	persister      *raft.Persister
	snapshotMu     sync.Mutex

	// the kv database
	dataBaseLock sync.RWMutex
	dataBase     map[int]map[string]string
	//used to store the sending data
	sendingDataBase     map[int]*installPackage
	sendingDataBaseLock sync.Mutex

	// the info of each client
	mapMu             sync.RWMutex
	clientInfoMap     map[int32]*clientInfo
	clientInfoLockMap map[int32]*clientInfoLock

	// used to store the lastest config
	configLock sync.RWMutex
	config     shardctrler.Config

	// used to cache the leaderid of the group
	leaderIds  map[int]int
	leaderIdMu sync.RWMutex

	// used to avoid installing the same shardDataBase twice
	shardLevel     [shardctrler.NShards]int
	shardLevelLock ReadMapCondition
	shardLevelCond sync.Cond
}

const pullerSleepTime = time.Second / 10
const sendingCheckerSleepTime = time.Second / 2

// used to pass to the raft
type Op struct {
	OperationId int
	ConfigNum   int
	Args        interface{}
}

// used to store the last reply to avoid double excute
type clientInfo struct {
	NextCommandId int32
	LastReply     string
}

// used to seriablize every client's command
type clientInfoLock struct {
	RwMu sync.RWMutex
	Cond sync.Cond
}

type installPackage struct {
	ConfigNum     int
	TargetGroupId int
	TargetServers []string
	ShardDatabase map[string]string
}

func (kv *ShardKV) getClientInfo(clientId int32) (*clientInfo, *clientInfoLock) {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// return if the server is not a leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dGroup, "G%d receive Get ClientId = %d CommandId = %d Key = %s shard = %d\n", kv.gid, args.ClientId, args.CommandId, args.Key, args.Shard)

	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := kv.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	nextCommand := clientInfoT.NextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		reply.Value = clientInfoT.LastReply
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// check whether the server is reponsible for the shard
	kv.configLock.RLock()
	kv.dataBaseLock.RLock()
	if kv.config.Shards[args.Shard] != kv.gid {
		kv.configLock.RUnlock()
		kv.dataBaseLock.RUnlock()
		clientInfoLockT.RwMu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	configNum := kv.config.Num
	// check the database to see whether the shard has finished
	if _, ok := kv.dataBase[args.Shard]; !ok {
		kv.configLock.RUnlock()
		kv.dataBaseLock.RUnlock()
		clientInfoLockT.RwMu.Unlock()
		reply.Err = ErrWaitingForReconfig
		return
	}
	assert(kv.config.Shards[args.Shard] == kv.gid, "Wrong group\n")
	kv.configLock.RUnlock()
	kv.dataBaseLock.RUnlock()

	operation := Op{GetOp, configNum, *args}
	kv.rf.Start(operation)
	Debug(dLeader, "G%d start %v\n", kv.gid, operation)
	clientInfoLockT.RwMu.Unlock()
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
		kv.configLock.RLock()
		if configNum != kv.config.Num {
			kv.configLock.RUnlock()
			reply.Err = ErrWrongGroup
			return
		}
		kv.configLock.RUnlock()
		clientInfoLockT.Cond.Wait()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// return if the server is not a leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(dGroup, "G%d receive Get ClientId = %d CommandId = %d Key = %s shard = %d\n", kv.gid, args.ClientId, args.CommandId, args.Key, args.Shard)

	clientId := args.ClientId
	commandId := args.CommandId
	clientInfoT, clientInfoLockT := kv.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	nextCommand := clientInfoT.NextCommandId
	// avoid twice excute
	if commandId == nextCommand-1 {
		reply.Err = OK
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// receive stale RPC call
	if commandId < nextCommand-1 {
		reply.Err = ErrRepeat
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// check whether the server is reponsible for the shard
	kv.configLock.RLock()
	kv.dataBaseLock.RLock()
	if kv.config.Shards[args.Shard] != kv.gid {
		kv.configLock.RUnlock()
		kv.dataBaseLock.RUnlock()
		clientInfoLockT.RwMu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	configNum := kv.config.Num
	// check the database to see whether the shard has finished
	if _, ok := kv.dataBase[args.Shard]; !ok {
		kv.configLock.RUnlock()
		kv.dataBaseLock.RUnlock()
		clientInfoLockT.RwMu.Unlock()
		reply.Err = ErrWaitingForReconfig
		return
	}
	assert(kv.config.Shards[args.Shard] == kv.gid, "Wrong group\n")
	kv.configLock.RUnlock()
	kv.dataBaseLock.RUnlock()

	operation := Op{PutAppendOp, configNum, *args}
	kv.rf.Start(operation)
	Debug(dLeader, "G%d start %v\n", kv.gid, operation)
	clientInfoLockT.RwMu.Unlock()
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
		kv.configLock.RLock()
		if configNum != kv.config.Num {
			kv.configLock.RUnlock()
			reply.Err = ErrWrongGroup
			return
		}
		kv.configLock.RUnlock()
		clientInfoLockT.Cond.Wait()
	}
}

func (kv *ShardKV) MigrateInstall(args *MigrateInstallArgs, reply *MigrateInstallReply) {
	// return if the server is not a leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.shardLevelLock.rwMu.RLock()
	kv.configLock.RLock()
	curConfigNum := kv.config.Num
	if args.ConfigNum > kv.config.Num {
		kv.configLock.RUnlock()
		kv.shardLevelLock.rwMu.RUnlock()
		reply.Err = ErrWaitingForReconfig
		return
	}
	Debug(dLeader, "G%d receive MigrateInstall newConfigNum = %d curConfigNum = %d shard = %d\n", kv.gid, args.ConfigNum, kv.config.Num, args.Shard)
	kv.configLock.RUnlock()
	if args.ConfigNum <= kv.shardLevel[args.Shard] {
		kv.shardLevelLock.rwMu.RUnlock()
		reply.Err = OK
		return
	}
	// check that only a copy of one shard
	kv.dataBaseLock.RLock()
	if _, ok := kv.dataBase[args.Shard]; ok {
		panic("There are multiple same shard\n")
	}
	kv.dataBaseLock.RUnlock()
	kv.shardLevelLock.rwMu.RUnlock()

	operation := Op{MigrateInstallOp, curConfigNum, *args}
	kv.rf.Start(operation)
	Debug(dLeader, "G%d start MigrateInstall operation = %v\n", kv.gid, operation)
	reply.Err = OK
	kv.shardLevelCond.L.Lock()
	defer kv.shardLevelCond.L.Unlock()
	for !kv.killed() {
		if args.ConfigNum <= kv.shardLevel[args.Shard] {
			reply.Err = OK
			return
		}
		kv.configLock.RLock()
		if curConfigNum < kv.config.Num {
			kv.configLock.RUnlock()
			reply.Err = ErrConfigNum
			return
		}
		kv.configLock.RUnlock()
		kv.shardLevelCond.Wait()
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(UpdateConfigArgs{})
	labgob.Register(MigrateInstallArgs{})
	labgob.Register(DeleteSendingDatabaseOpArgs{})

	kv := new(ShardKV)
	// used to transfer from name to server
	kv.make_end = make_end
	// used to mark self
	kv.me = me
	kv.gid = gid
	// used to snapshot
	kv.maxraftstate = maxraftstate
	kv.currentIndex = 0
	kv.receiveLogSize = 0
	kv.persister = persister

	// to talk to the controller
	kv.ctrlers = ctrlers
	kv.ctrlerClient = shardctrler.MakeClerk(kv.ctrlers)

	kv.dataBase = make(map[int]map[string]string)
	kv.sendingDataBase = make(map[int]*installPackage)
	kv.shardLevelLock = ReadMapCondition{&sync.RWMutex{}}
	kv.shardLevelCond.L = &kv.shardLevelLock
	kv.leaderIds = make(map[int]int)

	kv.clientInfoMap = make(map[int32]*clientInfo)
	kv.clientInfoLockMap = make(map[int32]*clientInfoLock)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyCheaker()
	go kv.configPuller()
	go kv.sendAreaChecker()

	return kv
}

func (kv *ShardKV) applyCheaker() {
	for !kv.killed() {
		receiveMessage := <-kv.applyCh
		if receiveMessage.CommandValid && !receiveMessage.SnapshotValid {
			kv.handleApplyLog(&receiveMessage)
		} else if receiveMessage.SnapshotValid && !receiveMessage.CommandValid {
			kv.handleSnapshot(&receiveMessage)
		} else {
			panic("apply has mistake\n")
		}
	}
}

func (kv *ShardKV) handleApplyLog(receiveMessage *raft.ApplyMsg) {
	// if the CommandIndex not match, when receive stale apply after snapshot
	if receiveMessage.CommandIndex != kv.currentIndex+1 {
		return
	}
	kv.currentIndex++
	var operation Op = receiveMessage.Command.(Op)
	switch operation.OperationId {
	case GetOp:
		kv.getHandler(&operation)
	case PutAppendOp:
		kv.putAppendHandler(&operation)
	case UpdateConfigOp:
		kv.updateConfigHandler(&operation)
	case MigrateInstallOp:
		kv.migrateInstallHandler(&operation)
	case DeleteSendingDatabaseOp:
		kv.DeleteSendingDatabaseHandler(&operation)
	}
	kv.receiveLogSize += int(unsafe.Sizeof(*receiveMessage))
	kv.checkSnapshot()
}

func (kv *ShardKV) handleSnapshot(receiveMessage *raft.ApplyMsg) {
	kv.mapMu.Lock()
	kv.snapshotMu.Lock()
	kv.dataBaseLock.Lock()
	kv.configLock.Lock()
	kv.sendingDataBaseLock.Lock()
	defer kv.mapMu.Unlock()
	defer kv.snapshotMu.Unlock()
	defer kv.dataBaseLock.Unlock()
	defer kv.configLock.Unlock()
	defer kv.sendingDataBaseLock.Unlock()
	r := bytes.NewBuffer(receiveMessage.Snapshot)
	d := labgob.NewDecoder(r)
	var configTemp shardctrler.Config
	var clientInfoMapTemp map[int32]*clientInfo
	var databaseTemp map[int]map[string]string
	var sendingDataBaseTemp map[int]*installPackage
	if d.Decode(&configTemp) != nil || d.Decode(&clientInfoMapTemp) != nil || d.Decode(&databaseTemp) != nil || d.Decode(&sendingDataBaseTemp) != nil || d.Decode(&kv.shardLevel) != nil {
		panic("Read Snapshot fails\n")
	}
	kv.config = configTemp
	kv.currentIndex = receiveMessage.SnapshotIndex
	kv.clientInfoMap = clientInfoMapTemp
	kv.dataBase = databaseTemp
	kv.sendingDataBase = sendingDataBaseTemp
}

func (kv *ShardKV) getHandler(operation *Op) {
	args := operation.Args.(GetArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	configNum := operation.ConfigNum

	clientInfoT, clientInfoLockT := kv.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	// if the client come from other group, update the clientCommandId
	if commandId > clientInfoT.NextCommandId {
		clientInfoT.NextCommandId = commandId
	}
	//check command vaild
	if ok := kv.checkCommandVaild(configNum, commandId, clientInfoT.NextCommandId, clientInfoLockT); !ok {
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// do corresponding operation
	kv.dataBaseLock.RLock()
	shardDataBase := kv.dataBase[args.Shard]
	clientInfoT.LastReply = shardDataBase[args.Key]
	clientInfoT.NextCommandId++
	kv.dataBaseLock.RUnlock()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
	clientInfoLockT.RwMu.Unlock()
}

func (kv *ShardKV) putAppendHandler(operation *Op) {
	args := operation.Args.(PutAppendArgs)
	clientId := args.ClientId
	commandId := args.CommandId
	configNum := operation.ConfigNum

	clientInfoT, clientInfoLockT := kv.getClientInfo(clientId)
	clientInfoLockT.RwMu.Lock()
	// if the client come from other group, update the clientCommandId
	if commandId > clientInfoT.NextCommandId {
		clientInfoT.NextCommandId = commandId
	}
	//check command vaild
	if ok := kv.checkCommandVaild(configNum, commandId, clientInfoT.NextCommandId, clientInfoLockT); !ok {
		clientInfoLockT.RwMu.Unlock()
		return
	}
	// do corresponding operation
	kv.dataBaseLock.Lock()
	shardDataBase := kv.dataBase[args.Shard]
	// do operation
	switch args.Op {
	case PutOp:
		shardDataBase[args.Key] = args.Value
	case AppendOp:
		value := shardDataBase[args.Key]
		shardDataBase[args.Key] = value + args.Value
	}
	clientInfoT.NextCommandId++
	kv.dataBaseLock.Unlock()
	// inform the waited RPC
	clientInfoLockT.Cond.Broadcast()
	clientInfoLockT.RwMu.Unlock()
}

func (kv *ShardKV) updateConfigHandler(operation *Op) {
	args := operation.Args.(UpdateConfigArgs)
	kv.configLock.Lock()
	defer kv.configLock.Unlock()
	if operation.ConfigNum != kv.config.Num {
		return
	}

	newConfigNum := args.NewConfig.Num

	assert(newConfigNum >= kv.config.Num, "configNum not matches newConfigNum = %d curConfigNum = %d\n", newConfigNum, kv.config.Num)

	Debug(dLeader, "G%d S%d Update config from %d to %d\n", kv.gid, kv.me, kv.config.Num, args.NewConfig.Num)
	kv.config = args.NewConfig
	// initialize the database
	if newConfigNum == 1 {
		kv.initDataBase()
	}
	// send shard that don's belong to the group
	kv.transferShards()
}

func (kv *ShardKV) migrateInstallHandler(operation *Op) {
	args := operation.Args.(MigrateInstallArgs)
	kv.configLock.RLock()
	if operation.ConfigNum < kv.config.Num {
		kv.configLock.RUnlock()
		kv.shardLevelCond.Broadcast()
		return
	}
	assert(operation.ConfigNum == kv.config.Num, "GG")
	kv.configLock.RUnlock()
	kv.shardLevelLock.rwMu.Lock()
	if args.ConfigNum <= kv.shardLevel[args.Shard] {
		kv.shardLevelLock.rwMu.Unlock()
		kv.shardLevelCond.Broadcast()
		return
	}
	kv.shardLevel[args.Shard] = args.ConfigNum
	kv.dataBaseLock.Lock()
	kv.configLock.RLock()
	Debug(dLeader, "G%d S%d install shard = %d database = %v", kv.gid, kv.me, args.Shard, args.DataBaseShard)
	if kv.config.Shards[args.Shard] == kv.gid {
		// if the shard belong to this group
		if _, ok := kv.dataBase[args.Shard]; !ok {
			kv.dataBase[args.Shard] = make(map[string]string)
			for i, v := range args.DataBaseShard {
				kv.dataBase[args.Shard][i] = v
			}
		} else {
			panic("install repeat shard\n")
		}
	} else {
		// need to transfor the shard
		var reSendShard map[string]string
		kv.sendingDataBaseLock.Lock()
		reSendShard = make(map[string]string)
		for i, v := range args.DataBaseShard {
			reSendShard[i] = v
		}
		targetGroup := kv.config.Shards[args.Shard]
		kv.sendingDataBase[args.Shard] = &installPackage{kv.config.Num, targetGroup, kv.config.Groups[targetGroup], reSendShard}
		kv.sendingDataBaseLock.Unlock()
	}

	kv.configLock.RUnlock()
	kv.dataBaseLock.Unlock()
	kv.shardLevelLock.rwMu.Unlock()
	kv.shardLevelCond.Broadcast()
}
func (kv *ShardKV) DeleteSendingDatabaseHandler(operation *Op) {
	args := operation.Args.(DeleteSendingDatabaseOpArgs)
	kv.configLock.Lock()
	if operation.ConfigNum != kv.config.Num {
		kv.configLock.Unlock()
		return
	}
	kv.configLock.Unlock()
	kv.sendingDataBaseLock.Lock()
	bag, ok := kv.sendingDataBase[args.ShardNum]
	if !ok {
		Debug(dGroup, "G%d fail to delete the sending area shard = %d for it has been deleted\n", kv.gid, args.ShardNum)
		kv.sendingDataBaseLock.Unlock()
		return
	}
	if bag.ConfigNum != args.ConfigNum {
		Debug(dGroup, "G%d fail to delete the sending area shard = %d for it is stale\n", kv.gid, args.ShardNum)
		kv.sendingDataBaseLock.Unlock()
		return
	}
	Debug(dGroup, "G%d delete sendingDatabase shard = %d config = %d\n", kv.gid, args.ShardNum, args.ConfigNum)
	delete(kv.sendingDataBase, args.ShardNum)
	kv.sendingDataBaseLock.Unlock()
}

func (kv *ShardKV) checkCommandVaild(configNum int, commandId int32, nextCommandId int32, clientInfoLockT *clientInfoLock) bool {
	// check wether the config Num match the current config Num to avoid twice excute
	if ok := kv.handlerCheckConfigNum(configNum, clientInfoLockT); !ok {
		clientInfoLockT.Cond.Broadcast()
		return false
	}
	// check whether the id matches the clientId to avoid twice excute
	if nextCommandId != commandId {
		clientInfoLockT.Cond.Broadcast()
		return false
	}
	return true
}

func (kv *ShardKV) handlerCheckConfigNum(configNum int, clientInfoLockT *clientInfoLock) bool {
	kv.configLock.RLock()
	if configNum != kv.config.Num {
		kv.configLock.RUnlock()
		return false
	}
	kv.configLock.RUnlock()
	return true
}

func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.configLock.RLock()
			preConfigNum := kv.config.Num
			kv.configLock.RUnlock()
			var newConfig shardctrler.Config
			if preConfigNum == 0 {
				newConfig = kv.ctrlerClient.Query(1)
			} else {
				newConfig = kv.ctrlerClient.Query(-1)
			}
			if preConfigNum != newConfig.Num {

				assert(preConfigNum < newConfig.Num, "Not possible configNum preConfigNum = %d newConfigNum = %d\n", preConfigNum, newConfig.Num)

				Debug(dLeader, "G%d start UpdataConfigOp configNum = %d pre = %d\n", kv.gid, newConfig.Num, preConfigNum)
				kv.rf.Start(Op{UpdateConfigOp, preConfigNum, UpdateConfigArgs{newConfig}})
			}
		}
		time.Sleep(pullerSleepTime)
	}
}

func (kv *ShardKV) checkSnapshot() {
	// if there is no need to snapshot
	if kv.maxraftstate == -1 || kv.receiveLogSize < kv.maxraftstate {
		return
	}
	kv.receiveLogSize = 0
	kv.mapMu.RLock()
	kv.snapshotMu.Lock()
	kv.dataBaseLock.RLock()
	kv.configLock.RLock()
	defer kv.mapMu.RUnlock()
	defer kv.snapshotMu.Unlock()
	defer kv.dataBaseLock.RUnlock()
	defer kv.configLock.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.clientInfoMap)
	e.Encode(kv.dataBase)
	e.Encode(kv.sendingDataBase)
	e.Encode(kv.shardLevel)
	kv.rf.Snapshot(kv.currentIndex, w.Bytes())
}

func (kv *ShardKV) transferShards() {
	kv.dataBaseLock.Lock()
	kv.sendingDataBaseLock.Lock()
	for shardNum, database := range kv.dataBase {
		groupId := kv.config.Shards[shardNum]
		if groupId != kv.gid {
			delete(kv.dataBase, shardNum)
			kv.sendingDataBase[shardNum] = &installPackage{kv.config.Num, groupId, kv.config.Groups[groupId], database}
			assert(len(kv.config.Groups[groupId]) != 0, "Groups = nil groupId = %d\n", groupId)
		}
	}
	kv.dataBaseLock.Unlock()
	kv.sendingDataBaseLock.Unlock()
}

func (kv *ShardKV) initDataBase() {
	kv.dataBaseLock.Lock()
	for i, v := range kv.config.Shards {
		if v == kv.gid {
			kv.dataBase[i] = make(map[string]string)
		}
	}
	kv.dataBaseLock.Unlock()
}

func (kv *ShardKV) sendAreaChecker() {
	for !kv.killed() {
		time.Sleep(sendingCheckerSleepTime)
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.sendingDataBaseLock.Lock()
			for shardNum, bag := range kv.sendingDataBase {
				Debug(dGroup, "G%d send bag targetGroup = %d shard = %d config = %d map = %v\n", kv.gid, bag.TargetGroupId, shardNum, bag.ConfigNum, bag.ShardDatabase)
				go kv.sendshard(shardNum, bag)
			}
			kv.sendingDataBaseLock.Unlock()
		}
	}
}

package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

const waitTimeReSend time.Duration = time.Second / 3
const waitTimeChangeLeader time.Duration = time.Second / 10
const waitInstall time.Duration = time.Second / 3

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm         *shardctrler.Clerk
	configLock sync.RWMutex
	config     shardctrler.Config
	make_end   func(string) *labrpc.ClientEnd

	// used to cache the leaderid of the group
	leaderIds  [shardctrler.NShards]int
	leaderIdMu sync.RWMutex

	// the ClientId which is used to distinguish every client
	clientId  int32
	commandId int32
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = ck.sm.GetClientId()
	ck.commandId = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.commandId++
	shard := key2shard(key)
	Debug(dClient, "C%d commandId = %d request Get key = %s shard = %d\n", ck.clientId, ck.commandId, key, shard)
	args := &GetArgs{key, shard, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[GetReply])
	// get the servers
	servers := ck.getServers(shard)
	// send the first go rutine
	var ret string
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardKV.Get", shard, servers)
loop:
	for {
		select {
		case receive := <-applyCh:
			if receive == nil {
				ck.changeLeaderIdUnVaild(shard, servers)
				go rpcSender(ck, args, applyCh, "ShardKV.Get", shard, servers)
				continue
			}
			// if the RPC is invaild
			if ck.getHandler(receive, &ret, &servers, shard) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardKV.Get", shard, servers)
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardKV.Get", shard, servers)
			threadNums++
		}
	}
	go WaitForThreads(threadNums, applyCh)
	return ret
}

func (ck *Clerk) getHandler(receive *replyStruct[GetReply], ret *string, servers *[]string, shard int) bool {
	switch receive.reply.Err {
	case OK:
		*ret = receive.reply.Value
		return true
	case ErrWrongLeader:
		ck.changeLeaderId(receive.preLeader, shard, *servers)
		return false
	case ErrWrongGroup:
		ck.config = ck.sm.Query(-1)
		*servers = ck.config.Groups[ck.config.Shards[shard]]
		return false
	case ErrWaitingForReconfig:
		time.Sleep(waitInstall)
		return false
	case ErrRepeat:
		panic("receive repeat answer\n")
	default:
		panic("receive unexpected err\n")
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op int) {
	ck.commandId++
	shard := key2shard(key)
	Debug(dClient, "C%d commandId = %d request %d key = %s value = %s shard = %d\n", ck.clientId, ck.commandId, op, key, value, shard)
	args := &PutAppendArgs{key, shard, value, op, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[PutAppendReply])
	// get the servers
	servers := ck.getServers(shard)
	// send the first go rutine
	var ret string
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardKV.PutAppend", shard, servers)
loop:
	for {
		select {
		case receive := <-applyCh:
			if receive == nil {
				ck.changeLeaderIdUnVaild(shard, servers)
				go rpcSender(ck, args, applyCh, "ShardKV.PutAppend", shard, servers)
				continue
			}
			// if the RPC is invaild
			if ck.putAppendHandler(receive, &ret, &servers, shard) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardKV.PutAppend", shard, servers)
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardKV.PutAppend", shard, servers)
			threadNums++
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) putAppendHandler(receive *replyStruct[PutAppendReply], ret *string, servers *[]string, shard int) bool {
	switch receive.reply.Err {
	case OK:
		return true
	case ErrWrongLeader:
		ck.changeLeaderId(receive.preLeader, shard, *servers)
		return false
	case ErrWrongGroup:
		ck.config = ck.sm.Query(-1)
		*servers = ck.config.Groups[ck.config.Shards[shard]]
		return false
	case ErrWaitingForReconfig:
		time.Sleep(waitInstall)
		return false
	case ErrRepeat:
		panic("receive repeat answer\n")
	default:
		panic("receive unexpected err\n")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}

func (ck *Clerk) changeLeaderId(leaderId int, shard int, servers []string) {
	ck.configLock.RLock()
	groupLen := len(servers)
	ck.configLock.RUnlock()
	ck.leaderIdMu.Lock()
	if leaderId == ck.leaderIds[shard] {
		// change to another server
		ck.leaderIds[shard] = int(nrand()) % groupLen
		if ck.leaderIds[shard] == leaderId {
			ck.leaderIds[shard] = (leaderId + 1) % groupLen
		}
	}
	ck.leaderIdMu.Unlock()
	time.Sleep(waitTimeChangeLeader)
}

func (ck *Clerk) changeLeaderIdUnVaild(shard int, servers []string) {
	ck.configLock.RLock()
	groupLen := len(servers)
	ck.configLock.RUnlock()
	ck.leaderIdMu.Lock()
	preLeaderId := ck.leaderIds[shard]
	// change to another server
	newLeaderId := int(nrand()) % groupLen
	if newLeaderId == preLeaderId {
		newLeaderId = (newLeaderId + 1) % groupLen
	}
	ck.leaderIds[shard] = newLeaderId
	ck.leaderIdMu.Unlock()
}

type replyStruct[T GetReply | PutAppendReply | MigrateInstallReply] struct {
	reply     T
	preLeader int
}

func rpcSender[argsT GetArgs | PutAppendArgs, replyT GetReply | PutAppendReply](ck *Clerk, args *argsT, applyCh chan *replyStruct[replyT], functionName string, shard int, servers []string) {
	var reply replyStruct[replyT]
	ck.leaderIdMu.RLock()
	reply.preLeader = ck.leaderIds[shard]
	ck.leaderIdMu.RUnlock()
	ok := ck.make_end(servers[reply.preLeader]).Call(functionName, args, &reply.reply)
	if !ok {
		// change the leaderId and try again if the package lost
		applyCh <- nil
		return
	}
	applyCh <- &reply
}

type replyChan[T interface {
	GetReply | PutAppendReply | MigrateInstallReply
}] chan *replyStruct[T]

func WaitForThreads[T interface {
	GetReply | PutAppendReply | MigrateInstallReply
}](threadNums int, applyCh replyChan[T]) {
	if threadNums > 0 {
		for {
			<-applyCh
			threadNums--
			if threadNums == 0 {
				return
			}
		}
	}
}

func (ck *Clerk) getServers(shard int) []string {
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			return servers
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

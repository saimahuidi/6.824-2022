package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var waitTimeReSend time.Duration = time.Second / 3
var waitTimeChangeLeader time.Duration = time.Second / 10

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	// the ClientId which is used to distinguish every client
	clientId  int32
	commandId int32

	// used to cache the recent leader
	leaderId int
	leaderMu sync.RWMutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = int(nrand()) % len(ck.servers)
	ck.commandId = -1
	ck.clientId = -1
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	if ck.clientId == -1 {
		ck.getClientId()
	}
	ck.commandId++
	args := &QueryArgs{num, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[QueryReply])
	Debug(dClient, "C%d commandId = %d request Query num = %d\n", ck.clientId, ck.commandId, num)
	var ret *Config
	// send the first go rutine
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardCtrler.Query")
loop:
	for {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			ret = ck.queryHandler(receive)
			if ret != nil {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardCtrler.Query")
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardCtrler.Query")
		}
	}
	go WaitForThreads(threadNums, applyCh)
	return *ret
}

func (ck *Clerk) queryHandler(receive *replyStruct[QueryReply]) *Config {
	switch receive.reply.Err {
	case OK:
		return &receive.reply.Config
	case ErrWrongLeader:
		ck.ChangeLeaderId(receive.preLeader)
		return nil
	default:
		panic("wrong err\n")
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	if ck.clientId == -1 {
		ck.getClientId()
	}
	ck.commandId++
	args := &JoinArgs{servers, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[JoinReply])
	Debug(dClient, "C%d commandId = %d request Join servers = %v\n", ck.clientId, ck.commandId, servers)
	// send the first go rutine
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardCtrler.Join")
loop:
	for {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			if ck.joinHandler(receive) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardCtrler.Join")
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardCtrler.Join")
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) joinHandler(receive *replyStruct[JoinReply]) bool {
	switch receive.reply.Err {
	case OK:
		return true
	case ErrWrongLeader:
		ck.ChangeLeaderId(receive.preLeader)
		return false
	default:
		panic("wrong err\n")
	}
}

func (ck *Clerk) Leave(gids []int) {
	if ck.clientId == -1 {
		ck.getClientId()
	}
	ck.commandId++
	args := &LeaveArgs{gids, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[LeaveReply])
	Debug(dClient, "C%d commandId = %d request Leave gids = %v\n", ck.clientId, ck.commandId, gids)
	// send the first go rutine
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardCtrler.Leave")
loop:
	for {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			if ck.leaveHandler(receive) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardCtrler.Leave")
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardCtrler.Leave")
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) leaveHandler(receive *replyStruct[LeaveReply]) bool {
	switch receive.reply.Err {
	case OK:
		return true
	case ErrWrongLeader:
		ck.ChangeLeaderId(receive.preLeader)
		return false
	default:
		panic("wrong err\n")
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	if ck.clientId == -1 {
		ck.getClientId()
	}
	ck.commandId++
	args := &MoveArgs{shard, gid, ck.clientId, ck.commandId}
	applyCh := make(chan *replyStruct[MoveReply])
	Debug(dClient, "C%d commandId = %d request Move shard = %d gid = %d\n", ck.clientId, ck.commandId, shard, gid)
	// send the first go rutine
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardCtrler.Move")
loop:
	for {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			if ck.moveHandler(receive) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardCtrler.Move")
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardCtrler.Move")
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) moveHandler(receive *replyStruct[MoveReply]) bool {
	switch receive.reply.Err {
	case OK:
		return true
	case ErrWrongLeader:
		ck.ChangeLeaderId(receive.preLeader)
		return false
	default:
		panic("wrong err\n")
	}
}

func (ck *Clerk) getClientId() {
	args := &GetClientIdArgs{}
	applyCh := make(chan *replyStruct[GetClientIdReply])
	Debug(dClient, "some client request GetClientId")
	// send the first go rutine
	threadNums := 1
	go rpcSender(ck, args, applyCh, "ShardCtrler.GetClientId")
loop:
	for {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			if ck.getClientIdHandler(receive) {
				threadNums--
				break loop
			} else {
				go rpcSender(ck, args, applyCh, "ShardCtrler.GetClientId")
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			go rpcSender(ck, args, applyCh, "ShardCtrler.GetClientId")
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) getClientIdHandler(receive *replyStruct[GetClientIdReply]) bool {
	var ret bool
	switch receive.reply.Err {
	case OK:
		// no need to lock since only the first reply will be handled
		if ck.clientId == -1 {
			ck.clientId = receive.reply.ClientId
		} else {
			panic("receive clientId twice\n")
		}
		ret = true
	case ErrWrongLeader:
		ck.ChangeLeaderId(receive.preLeader)
		ret = false
	default:
		panic("Not vaild reply.err!\n")
	}
	return ret
}

type replyStruct[T JoinReply | GetClientIdReply | MoveReply | LeaveReply | QueryReply] struct {
	reply     T
	preLeader int
}

func rpcSender[argsT JoinArgs | GetClientIdArgs | MoveArgs | LeaveArgs | QueryArgs, replyT JoinReply | GetClientIdReply | MoveReply | LeaveReply | QueryReply](ck *Clerk, args *argsT, applyCh chan *replyStruct[replyT], functionName string) {
	for {
		var reply replyStruct[replyT]
		ck.leaderMu.RLock()
		reply.preLeader = ck.leaderId
		ck.leaderMu.RUnlock()
		ok := ck.servers[reply.preLeader].Call(functionName, args, &reply.reply)
		if !ok {
			// change the leaderId and try again if the package lost
			ck.ChangeLeaderIdUnVaild()
			continue
		}
		applyCh <- &reply
		return
	}
}

type replyChan[T interface {
	JoinReply | GetClientIdReply | MoveReply | LeaveReply | QueryReply
}] chan *replyStruct[T]

func WaitForThreads[T interface {
	JoinReply | GetClientIdReply | MoveReply | LeaveReply | QueryReply
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

func (ck *Clerk) ChangeLeaderId(leaderId int) int {
	ck.leaderMu.Lock()
	if leaderId != ck.leaderId {
		leaderId = ck.leaderId
	} else {
		// change to another server
		ck.leaderId = int(nrand()) % len(ck.servers)
		if ck.leaderId == leaderId {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		leaderId = ck.leaderId
	}
	ck.leaderMu.Unlock()
	time.Sleep(waitTimeChangeLeader)
	return leaderId
}

func (ck *Clerk) ChangeLeaderIdUnVaild() {
	ck.leaderMu.Lock()
	preLeaderId := ck.leaderId
	// change to another server
	ck.leaderId = int(nrand()) % len(ck.servers)
	if ck.leaderId == preLeaderId {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
	ck.leaderMu.Unlock()
}

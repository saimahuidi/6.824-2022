package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

var ClientId int32 = -1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// used to sign the unique client
	clientId int32
	// used to sign the unique command to avoid twice-execute
	commandId int32
	// used to cache the recent leader
	leaderId int
	leaderMu sync.RWMutex
}

type ConfirmEntry struct {
	valid bool
	value string
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
	ck.clientId = atomic.AddInt32(&ClientId, 1)
	ck.commandId = -1
	ck.leaderId = int(nrand()) % len(ck.servers)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
	commandId := atomic.AddInt32(&ck.commandId, 1)
	applyCh := make(chan ConfirmEntry)
	result := ""
	DeBug(dClient, "C%d commandId = %d calls   Get\t on Key = %s\n", ck.clientId, commandId, key)
	// send the first go rutine
	threadNums := 1
	go ck.GetHandler(key, commandId, applyCh)
loop:
	for {
		select {
		case reply := <-applyCh:
			threadNums--
			if reply.valid {
				result = reply.value
				break loop
			} else {
				ck.ChangeLeaderIdUnVaild()
			}
		case <-time.After(time.Second / 2):
			// send another request if timeout
			go ck.GetHandler(key, commandId, applyCh)
			threadNums++
		}
	}
	go ck.WaitForThreads(threadNums, applyCh)
	// println("result = ", result)
	return result
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	commandId := atomic.AddInt32(&ck.commandId, 1)
	applyCh := make(chan ConfirmEntry)
	DeBug(dClient, "C%d commandID = %d calls   %s\t on Key = %s value = %s\n", ck.clientId, commandId, op, key, value)
	// send the first go rutine
	threadNums := 1
	go ck.PutAppendHandler(key, value, op, commandId, applyCh)
loop:
	for {
		select {
		case reply := <-applyCh:
			threadNums--
			if reply.valid {
				break loop
			} else {
				ck.ChangeLeaderIdUnVaild()
			}
		case <-time.After(time.Second / 2):
			// send another request if timeout
			go ck.PutAppendHandler(key, value, op, commandId, applyCh)
			threadNums++
		}
	}
	go ck.WaitForThreads(threadNums, applyCh)
}

func (ck *Clerk) GetHandler(key string, commandId int32, applych chan ConfirmEntry) {
	args := GetArgs{key, ck.clientId, commandId}
	ck.leaderMu.RLock()
	leaderId := ck.leaderId
	ck.leaderMu.RUnlock()
loop:
	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			applych <- ConfirmEntry{false, ""}
			return
		}
		switch reply.Err {
		case OK:
			// get the result successfully
			applych <- ConfirmEntry{true, reply.Value}
			break loop
		case ErrNoKey:
			// the key doesn't exist
			applych <- ConfirmEntry{true, ""}
			break loop
		case ErrRepeat:
			// the command has been excuted
			applych <- ConfirmEntry{true, reply.Value}
			break loop
		case ErrWrongLeader:
			leaderId = ck.ChangeLeaderId(leaderId)
			time.Sleep(time.Microsecond * 100)
		}
	}
}

func (ck *Clerk) PutAppendHandler(key string, value string, op string, commandId int32, applych chan ConfirmEntry) {
	args := PutAppendArgs{key, value, op, ck.clientId, commandId}
	ck.leaderMu.RLock()
	leaderId := ck.leaderId
	ck.leaderMu.RUnlock()
loop:
	for {
		reply := PutAppendReply{}
		// keep trying if the package lost
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			applych <- ConfirmEntry{false, ""}
			return
		}
		switch reply.Err {
		case OK:
			// append successful
			applych <- ConfirmEntry{true, ""}
			break loop
		case ErrWrongLeader:
			leaderId = ck.ChangeLeaderId(leaderId)
			time.Sleep(time.Microsecond * 100)
		}
	}
}

func (ck *Clerk) WaitForThreads(threadNums int, applyCh chan ConfirmEntry) {
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

package shardkv

import (
	"log"
	"sync"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const threadMaxNum = 5

const (
	OK = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrConfigNum
	ErrRepeat
	ErrWaitingForReconfig
	ErrStaleConfig
)

const (
	GetOp = iota
	PutOp
	AppendOp
	PutAppendOp
	UpdateConfigOp
	MigrateInstallOp
	MigrateConfirmOp
	DeleteSendingDatabaseOp
)

type Err int

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Shard     int
	Value     string
	Op        int // "Put" or "Append"
	ClientId  int32
	CommandId int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Shard     int
	ClientId  int32
	CommandId int32
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateConfigArgs struct {
	NewConfig shardctrler.Config
}

type MigrateInstallArgs struct {
	ConfigNum     int
	Shard         int
	DataBaseShard map[string]string
}

type MigrateInstallReply struct {
	Err Err
}

type DeleteSendingDatabaseOpArgs struct {
	ShardNum  int
	ConfigNum int
}

func assert(content bool, format string, a ...interface{}) {
	if !content {
		log.Printf(format, a...)
		panic("")
	}
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

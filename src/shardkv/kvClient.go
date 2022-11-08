package shardkv

import (
	"time"
)

func (kv *ShardKV) sendshard(shardNum int, bag *installPackage) {
	applyCh := make(chan *replyStruct[MigrateInstallReply])
	threadNums := 1
	kv.sendingDataBaseLock.Lock()
	args := MigrateInstallArgs{bag.ConfigNum, shardNum, bag.ShardDatabase}
	bagBackUp := *bag
	kv.sendingDataBaseLock.Unlock()
	go kv.sendShardSender(&args, applyCh, &bagBackUp)
loop:
	for !kv.killed() {
		select {
		case receive := <-applyCh:
			// if the RPC is invaild
			if kv.sendshardHandler(receive, shardNum, bag) {
				threadNums--
				break loop
			} else {
				go kv.sendShardSender(&args, applyCh, &bagBackUp)
			}
		case <-time.After(waitTimeReSend):
			// send another request if timeout
			// to avoid data race
			if threadNums < threadMaxNum {
				go kv.sendShardSender(&args, applyCh, &bagBackUp)
				threadNums++
			}
		}
	}
	go WaitForThreads(threadNums, applyCh)
}

func (kv *ShardKV) sendShardSender(args *MigrateInstallArgs, applyCh chan *replyStruct[MigrateInstallReply], bag *installPackage) {
	var reply replyStruct[MigrateInstallReply]
	kv.leaderIdMu.RLock()
	reply.preLeader = kv.leaderIds[bag.TargetGroupId]
	kv.leaderIdMu.RUnlock()
	ok := kv.make_end(bag.TargetServers[reply.preLeader]).Call("ShardKV.MigrateInstall", args, &reply.reply)
	if !ok {
		// change the leaderId and try again if the package lost
		reply.reply.Err = ErrWrongLeader
	}
	applyCh <- &reply
}

func (kv *ShardKV) sendshardHandler(receive *replyStruct[MigrateInstallReply], shardNum int, bag *installPackage) bool {
	switch receive.reply.Err {
	case OK:
		kv.configLock.RLock()
		curConfigNum := kv.config.Num
		kv.configLock.RUnlock()
		kv.rf.Start(Op{DeleteSendingDatabaseOp, curConfigNum, DeleteSendingDatabaseOpArgs{shardNum, bag.ConfigNum}})
		return true
	case ErrWrongLeader:
		kv.changeLeaderId(receive.preLeader, bag)
		return false
	case ErrConfigNum:
		return false
	case ErrWaitingForReconfig:
		time.Sleep(waitTimeReSend)
		return false
	}
	return false
}

func (kv *ShardKV) changeLeaderId(preLeaderId int, bag *installPackage) {
	groupLen := len(bag.TargetServers)
	kv.leaderIdMu.Lock()
	// change to another server
	newLeaderId := int(nrand()) % groupLen
	if newLeaderId == preLeaderId {
		newLeaderId = (newLeaderId + 1) % groupLen
	}
	kv.leaderIds[bag.TargetGroupId] = newLeaderId
	kv.leaderIdMu.Unlock()
}

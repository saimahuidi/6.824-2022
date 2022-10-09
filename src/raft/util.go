package raft

import "log"

// Debugging
const DebugEnable = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugEnable {
		log.Printf(format, a...)
	}
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

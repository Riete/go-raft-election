package election

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

type LeaderTracker struct {
	newLeader  chan raft.ServerAddress
	notifyWait *time.Ticker
	notifying  *atomic.Bool
	leaderNow  *atomic.Pointer[raft.ServerAddress]
}

func (l *LeaderTracker) notify() {
	<-l.notifyWait.C
	l.notifying.Store(false)
	l.notifyWait.Stop()
	l.newLeader <- *l.leaderNow.Load()
}

func (l *LeaderTracker) Apply(log *raft.Log) interface{} {
	leaderNow := raft.ServerAddress(log.Data)
	l.leaderNow.Store(&leaderNow)
	if !l.notifying.Load() {
		l.notifying.Store(true)
		l.notifyWait = time.NewTicker(time.Second)
		go l.notify()
	} else {
		l.notifyWait.Reset(time.Second)
	}
	return nil
}

func (l *LeaderTracker) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (l *LeaderTracker) Restore(snapshot io.ReadCloser) error {
	return nil
}

func NewLeaderTracker(newLeader chan raft.ServerAddress) raft.FSM {
	return &LeaderTracker{
		newLeader: newLeader,
		notifying: new(atomic.Bool),
		leaderNow: new(atomic.Pointer[raft.ServerAddress]),
	}
}

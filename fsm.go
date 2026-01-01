package election

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

type LeaderTracker struct {
	newLeader chan raft.ServerAddress
	wait      *time.Ticker
	delaying  *atomic.Bool
	leaderNow *atomic.Pointer[raft.ServerAddress]
}

func (l *LeaderTracker) sendNewLeaderEvent() {
	<-l.wait.C
	l.delaying.Store(false)
	l.wait.Stop()
	leaderAddress := *l.leaderNow.Load()
	l.newLeader <- leaderAddress
}

func (l *LeaderTracker) Apply(lg *raft.Log) interface{} {
	leaderNow := raft.ServerAddress(lg.Data)
	l.leaderNow.Store(&leaderNow)
	if !l.delaying.Load() {
		l.delaying.Store(true)
		l.wait = time.NewTicker(time.Second)
		go l.sendNewLeaderEvent()
	} else {
		l.wait.Reset(time.Second)
	}
	return nil
}

func (l *LeaderTracker) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (l *LeaderTracker) Restore(snapshot io.ReadCloser) error {
	return nil
}

func NewLeaderTracker(newLeader chan raft.ServerAddress) *LeaderTracker {
	return &LeaderTracker{
		newLeader: newLeader,
		delaying:  new(atomic.Bool),
		leaderNow: new(atomic.Pointer[raft.ServerAddress]),
	}
}

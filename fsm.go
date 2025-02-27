package election

import (
	"context"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

type LeaderTracker struct {
	leaderAddr   raft.ServerAddress
	newLeader    chan raft.ServerAddress
	cancelNotify context.CancelFunc
}

func (l *LeaderTracker) notifyLatest(newLeader raft.ServerAddress) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			l.newLeader <- newLeader
		}
	}()
	return cancel
}

func (l *LeaderTracker) Apply(log *raft.Log) interface{} {
	newLeader := raft.ServerAddress(log.Data)
	if l.leaderAddr != newLeader {
		if l.cancelNotify != nil {
			l.cancelNotify()
		}
		l.cancelNotify = l.notifyLatest(newLeader)
	}
	l.leaderAddr = newLeader
	return nil
}

func (l *LeaderTracker) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (l *LeaderTracker) Restore(snapshot io.ReadCloser) error {
	return nil
}

func NewLeaderTracker(newLeader chan raft.ServerAddress) raft.FSM {
	return &LeaderTracker{newLeader: newLeader}
}

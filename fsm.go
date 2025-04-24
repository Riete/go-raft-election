package election

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

type LeaderTracker struct {
	rw         sync.RWMutex
	newLeader  chan raft.ServerAddress
	notifyWait *time.Ticker
	notifying  *atomic.Bool
	leaderNow  *atomic.Pointer[raft.ServerAddress]
	receiver   map[int64]chan raft.ServerAddress
}

func (l *LeaderTracker) newReceiver() (int64, chan raft.ServerAddress) {
	l.rw.Lock()
	defer l.rw.Unlock()
	receiverId := time.Now().UnixNano()
	ch := make(chan raft.ServerAddress)
	l.receiver[receiverId] = ch
	return receiverId, ch
}

func (l *LeaderTracker) removeReceiver(receiverId int64) {
	l.rw.Lock()
	defer l.rw.Unlock()
	if ch, ok := l.receiver[receiverId]; ok {
		close(ch)
		delete(l.receiver, receiverId)
	}
}

func (l *LeaderTracker) notifyReceiver(leaderAddress raft.ServerAddress) {
	l.rw.RLock()
	defer l.rw.RUnlock()

	for _, ch := range l.receiver {
		go func(ch chan raft.ServerAddress) {
			// ignore send on closed channel panic error
			defer func() {
				_ = recover()
			}()
			ch <- leaderAddress
		}(ch)
	}
}

func (l *LeaderTracker) notifyCandidate(leaderAddress raft.ServerAddress) {
	l.newLeader <- leaderAddress
}

func (l *LeaderTracker) notify() {
	<-l.notifyWait.C
	l.notifying.Store(false)
	l.notifyWait.Stop()
	leaderAddress := *l.leaderNow.Load()
	go l.notifyReceiver(leaderAddress)
	go l.notifyCandidate(leaderAddress)
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

func NewLeaderTracker(newLeader chan raft.ServerAddress) *LeaderTracker {
	return &LeaderTracker{
		newLeader: newLeader,
		notifying: new(atomic.Bool),
		leaderNow: new(atomic.Pointer[raft.ServerAddress]),
		receiver:  make(map[int64]chan raft.ServerAddress),
	}
}

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
	watcher    map[int64]chan raft.ServerAddress
}

func (l *LeaderTracker) RegisterWatcher() (int64, chan raft.ServerAddress) {
	l.rw.Lock()
	defer l.rw.Unlock()
	watcherId := time.Now().UnixNano()
	ch := make(chan raft.ServerAddress)
	l.watcher[watcherId] = ch
	return watcherId, ch
}

func (l *LeaderTracker) DeregisterWatcher(watcherId int64) {
	l.rw.Lock()
	defer l.rw.Unlock()
	if ch, ok := l.watcher[watcherId]; ok {
		close(ch)
		delete(l.watcher, watcherId)
	}
}

func (l *LeaderTracker) notifyWatcher(leaderAddress raft.ServerAddress) {
	l.rw.RLock()
	defer l.rw.RUnlock()
	for _, ch := range l.watcher {
		go func(ch chan raft.ServerAddress) {
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
	go l.notifyWatcher(leaderAddress)
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
		watcher:   make(map[int64]chan raft.ServerAddress),
	}
}

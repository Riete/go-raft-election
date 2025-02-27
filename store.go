package election

import (
	"path/filepath"

	"github.com/hashicorp/raft"
	raftdb "github.com/hashicorp/raft-boltdb"
)

func createStore(f func() error, err error) error {
	if err != nil {
		return err
	}
	return f()
}

type Store struct {
	base     string
	retain   int
	Log      raft.LogStore
	Stable   raft.StableStore
	Snapshot raft.SnapshotStore
}

func (s *Store) createLogStore() error {
	var err error
	s.Log, err = raftdb.NewBoltStore(filepath.Join(s.base, "log.db"))
	return err
}

func (s *Store) createStableStore() error {
	var err error
	s.Stable, err = raftdb.NewBoltStore(filepath.Join(s.base, "stable.db"))
	return err
}

func (s *Store) createSnapshotStore() error {
	var err error
	s.Snapshot, err = raft.NewFileSnapshotStore(s.base, s.retain, nil)
	return err
}

// NewMemoryStore For testing or specified scenarios
func NewMemoryStore() *Store {
	return &Store{
		Log:      raft.NewInmemStore(),
		Stable:   raft.NewInmemStore(),
		Snapshot: raft.NewInmemSnapshotStore(),
	}
}

func NewFileStore(base string, retain int) (*Store, error) {
	var err error
	s := &Store{base: base, retain: retain}
	err = createStore(s.createSnapshotStore, err)
	err = createStore(s.createLogStore, err)
	err = createStore(s.createStableStore, err)
	return s, err
}

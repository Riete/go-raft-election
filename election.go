package election

import (
	"time"

	"github.com/hashicorp/raft"
)

type Candidate struct {
	s           *Store
	c           *Config
	r           *raft.Raft
	peersConfig []*Config
	promote     chan struct{}
	demote      chan struct{}
	newLeader   chan raft.ServerAddress
}

func (c *Candidate) Raft() *raft.Raft {
	return c.r
}

func (c *Candidate) init() error {
	advertiseAddr, err := c.c.AdvertiseAddr()
	if err != nil {
		return err
	}
	maxPool := c.c.TransportMaxPool
	if maxPool == 0 {
		maxPool = TransportDefaultMaxPool
	}
	timeout := c.c.TransportTimeout
	if timeout == 0 {
		timeout = TransportDefaultTimeout
	}
	trans, err := raft.NewTCPTransport(c.c.BindAddr(), advertiseAddr, maxPool, timeout, c.c.LogWriter)
	if err != nil {
		return err
	}
	c.r, err = raft.NewRaft(c.c.RaftConfig(), NewLeaderTracker(c.newLeader), c.s.Log, c.s.Stable, c.s.Snapshot, trans)
	if err == nil {
		go func() {
			for {
				if <-c.r.LeaderCh() {
					c.promote <- struct{}{}
					c.r.Apply([]byte(c.c.AdvertiseAddress()), time.Second)
				} else {
					c.demote <- struct{}{}
				}
			}
		}()
	}
	return err
}

func (c *Candidate) BootstrapCluster() {
	c.r.BootstrapCluster(MembersConfig(append(c.peersConfig, c.c)))
}

func (c *Candidate) Shutdown() raft.Future {
	return c.r.Shutdown()
}

func (c *Candidate) Leader() bool {
	return c.r.State() == raft.Leader
}

func (c *Candidate) State() raft.RaftState {
	return c.r.State()
}

func (c *Candidate) BecomeLeader() chan struct{} {
	return c.promote
}

func (c *Candidate) LoseLeader() chan struct{} {
	return c.demote
}

func (c *Candidate) OnNewLeader() chan raft.ServerAddress {
	return c.newLeader
}

func NewCandidate(s *Store, c *Config, peersConfig ...*Config) (*Candidate, error) {
	candidate := &Candidate{
		s:           s,
		c:           c,
		promote:     make(chan struct{}),
		demote:      make(chan struct{}),
		newLeader:   make(chan raft.ServerAddress),
		peersConfig: peersConfig,
	}
	return candidate, candidate.init()
}

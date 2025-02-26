package election

import (
	"github.com/hashicorp/raft"
)

type Candidate struct {
	s *Store
	c *Config
	r *raft.Raft
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
	trans, err := raft.NewTCPTransport(c.c.BindAddr(), advertiseAddr, maxPool, timeout, nil)
	if err != nil {
		return err
	}
	c.r, err = raft.NewRaft(c.c.RaftConfig(), nil, c.s.Log, c.s.Stable, c.s.Snapshot, trans)
	return err
}

func (c *Candidate) BootstrapCluster(members Members) {
	c.r.BootstrapCluster(c.c.ClusterMembersConfig(members))
}

func (c *Candidate) Shutdown() raft.Future {
	return c.r.Shutdown()
}

func (c *Candidate) Leader() bool {
	return c.r.State() == raft.Leader
}

func (c *Candidate) BecomeLeader() bool {
	return <-c.r.LeaderCh()
}

func (c *Candidate) LoseLeader() bool {
	return !c.BecomeLeader()
}

func NewCandidate(s *Store, c *Config) (*Candidate, error) {
	candidate := &Candidate{s: s, c: c}
	return candidate, candidate.init()
}

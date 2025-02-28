package election

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

type EventHandler struct {
	OnPromote   func()
	OnDemote    func()
	OnNewLeader func(raft.ServerAddress)
}

type Candidate struct {
	s           *Store
	c           *Config
	r           *raft.Raft
	peersConfig []*Config
	promote     chan struct{}
	demote      chan struct{}
	newLeader   chan raft.ServerAddress
	shutdown    context.CancelFunc
}

func (c *Candidate) Raft() *raft.Raft {
	return c.r
}

func (c *Candidate) init(ctx context.Context) error {
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
	trans, err := raft.NewTCPTransport(c.c.BindAddress(), advertiseAddr, maxPool, timeout, c.c.LogWriter)
	if err != nil {
		return err
	}
	c.r, err = raft.NewRaft(c.c.RaftConfig(), NewLeaderTracker(c.newLeader), c.s.Log, c.s.Stable, c.s.Snapshot, trans)
	if err == nil {
		go func() {
			for {
				select {
				case becomeLeader := <-c.r.LeaderCh():
					if becomeLeader {
						c.promote <- struct{}{}
						c.r.Apply([]byte(c.c.AdvertiseAddress()), time.Second)
					} else {
						c.demote <- struct{}{}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return err
}

func (c *Candidate) bootstrapCluster() {
	c.r.BootstrapCluster(MembersConfig(append(c.peersConfig, c.c)))
}

func (c *Candidate) Start() error {
	var ctx context.Context
	ctx, c.shutdown = context.WithCancel(context.Background())
	if err := c.init(ctx); err != nil {
		return err
	}
	c.bootstrapCluster()
	return nil
}

func (c *Candidate) Shutdown() error {
	c.shutdown()
	return c.r.Shutdown().Error()
}

func (c *Candidate) Leader() bool {
	return c.r.VerifyLeader().Error() == nil
}

func (c *Candidate) State() raft.RaftState {
	return c.r.State()
}

func (c *Candidate) RunEventLoop(ctx context.Context, handler EventHandler) {
	for {
		select {
		case <-c.promote:
			if handler.OnPromote != nil {
				handler.OnPromote()
			}
		case <-c.demote:
			if handler.OnDemote != nil {
				handler.OnDemote()
			}
		case newLeader := <-c.newLeader:
			if handler.OnNewLeader != nil {
				handler.OnNewLeader(newLeader)
			}
		case <-ctx.Done():
			return
		}
	}
}

func NewCandidate(s *Store, c *Config, peersConfig ...*Config) *Candidate {
	return &Candidate{
		s:           s,
		c:           c,
		promote:     make(chan struct{}),
		demote:      make(chan struct{}),
		newLeader:   make(chan raft.ServerAddress),
		peersConfig: peersConfig,
	}
}

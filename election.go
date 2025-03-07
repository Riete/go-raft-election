package election

import (
	"context"
	"slices"
	"sync"
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
	fsm         *LeaderTracker
	mu          sync.Mutex
	peersConfig raft.Configuration
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
	c.r, err = raft.NewRaft(c.c.RaftConfig(), c.fsm, c.s.Log, c.s.Stable, c.s.Snapshot, trans)
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
	_ = c.r.BootstrapCluster(c.peersConfig).Error()
}

func (c *Candidate) start() error {
	var ctx context.Context
	ctx, c.shutdown = context.WithCancel(context.Background())
	return c.init(ctx)
}

func (c *Candidate) Start() error {
	return c.start()
}

func (c *Candidate) StartCluster() error {
	if err := c.start(); err != nil {
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

func (c *Candidate) RegisterWatcher() (int64, chan raft.ServerAddress) {
	return c.fsm.RegisterWatcher()
}

func (c *Candidate) DeregisterWatcher(watcherId int64) {
	c.fsm.DeregisterWatcher(watcherId)
}

func (c *Candidate) Peers() []raft.Server {
	config := c.r.GetConfiguration()
	if config.Error() != nil {
		return nil
	}
	runtimeConfig := config.Configuration()
	c.peersConfig = runtimeConfig.Clone()
	slices.SortStableFunc(c.peersConfig.Servers, func(s1, s2 raft.Server) int {
		if s1.ID > s2.ID {
			return 1
		}
		return -1
	})
	return c.peersConfig.Servers
}

func (c *Candidate) LFPeers() (leader raft.Server, followers []raft.Server) {
	_, leaderId := c.r.LeaderWithID()
	for _, s := range c.Peers() {
		if s.ID == leaderId {
			leader = s
		} else {
			followers = append(followers, s)
		}
	}
	return
}

func (c *Candidate) AddPeer(config *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.r.AddVoter(config.ServerId, config.AdvertiseAddress(), 0, 0).Error()
}

func (c *Candidate) AddPeers(configs ...*Config) map[*Config]error {
	failed := make(map[*Config]error)
	for _, config := range configs {
		if err := c.AddPeer(config); err != nil {
			failed[config] = err
		}
	}
	return failed
}

func (c *Candidate) RemovePeer(config *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.r.RemoveServer(config.ServerId, 0, 0).Error()
}

func (c *Candidate) RemovePeers(configs ...*Config) map[*Config]error {
	failed := make(map[*Config]error)
	for _, config := range configs {
		if err := c.RemovePeer(config); err != nil {
			failed[config] = err
		}
	}
	return failed
}

func NewCandidate(s *Store, c *Config, peersConfig ...*Config) *Candidate {
	newLeader := make(chan raft.ServerAddress)
	return &Candidate{
		s:           s,
		c:           c,
		fsm:         NewLeaderTracker(newLeader),
		promote:     make(chan struct{}),
		demote:      make(chan struct{}),
		newLeader:   newLeader,
		peersConfig: PeersConfig(append(peersConfig, c)),
	}
}

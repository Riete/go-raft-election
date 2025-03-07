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
	store     *Store
	config    *Config
	rc        raft.Configuration
	raft      *raft.Raft
	fsm       *LeaderTracker
	mu        sync.Mutex
	once      sync.Once
	promote   chan struct{}
	demote    chan struct{}
	newLeader chan raft.ServerAddress
	shutdown  context.CancelFunc
}

func (c *Candidate) Raft() *raft.Raft {
	return c.raft
}

func (c *Candidate) init() error {
	var ctx context.Context
	ctx, c.shutdown = context.WithCancel(context.Background())
	advertiseAddr, err := c.config.AdvertiseAddr()
	if err != nil {
		return err
	}
	c.config.SetDefault()
	trans, err := raft.NewTCPTransport(
		c.config.BindAddress(), advertiseAddr, c.config.TransportMaxPool, c.config.TransportTimeout, c.config.LogWriter,
	)
	if err != nil {
		return err
	}
	c.raft, err = raft.NewRaft(c.config.RaftConfig(), c.fsm, c.store.Log, c.store.Stable, c.store.Snapshot, trans)
	if err == nil {
		go func() {
			for {
				select {
				case becomeLeader := <-c.raft.LeaderCh():
					if becomeLeader {
						c.promote <- struct{}{}
						c.raft.Apply([]byte(c.config.AdvertiseAddress()), time.Second)
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

func (c *Candidate) Startup() error {
	return c.init()
}

func (c *Candidate) Shutdown() error {
	c.shutdown()
	return c.raft.Shutdown().Error()
}

func (c *Candidate) BootstrapCluster() {
	c.once.Do(func() {
		_ = c.raft.BootstrapCluster(c.rc).Error()
	})
}

func (c *Candidate) Leader() bool {
	return c.raft.VerifyLeader().Error() == nil
}

func (c *Candidate) RegisterOnNewLeaderReceiver() (int64, chan raft.ServerAddress) {
	return c.fsm.newReceiver()
}

func (c *Candidate) DeregisterOnNewLeaderReceiver(watcherId int64) {
	c.fsm.removeReceiver(watcherId)
}

func (c *Candidate) Members() (leader raft.Server, followers []raft.Server) {
	config := c.raft.GetConfiguration()
	if config.Error() != nil {
		return
	}

	runtimeConfig := config.Configuration()
	c.rc = runtimeConfig.Clone()
	slices.SortStableFunc(c.rc.Servers, func(s1, s2 raft.Server) int {
		if s1.ID > s2.ID {
			return 1
		}
		return -1
	})

	_, leaderId := c.raft.LeaderWithID()
	for _, s := range c.rc.Servers {
		if s.ID == leaderId {
			leader = s
		} else {
			followers = append(followers, s)
		}
	}
	return
}

func (c *Candidate) AddMember(config *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raft.AddVoter(config.ServerId, config.AdvertiseAddress(), 0, 0).Error()
}

func (c *Candidate) RemoveMember(config *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raft.RemoveServer(config.ServerId, 0, 0).Error()
}

func (c *Candidate) TransferLeader() error {
	return c.raft.LeadershipTransfer().Error()
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

func NewCandidate(store *Store, config *Config, peers ...*Config) *Candidate {
	newLeader := make(chan raft.ServerAddress)
	return &Candidate{
		store:     store,
		config:    config,
		rc:        PeersConfig(append(peers, config)),
		fsm:       NewLeaderTracker(newLeader),
		promote:   make(chan struct{}),
		demote:    make(chan struct{}),
		newLeader: newLeader,
	}
}

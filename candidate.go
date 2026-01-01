package election

import (
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
	shutdown  chan struct{}
	handlers  map[int64]EventHandler
}

func (c *Candidate) Raft() *raft.Raft {
	return c.raft
}

func (c *Candidate) init() error {
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
						c.raft.Apply([]byte(c.config.AdvertiseAddress()), 5*time.Second)
						c.promote <- struct{}{}
					} else {
						c.demote <- struct{}{}
					}
				case <-c.shutdown:
					return
				}
			}
		}()
	}
	return err
}

func (c *Candidate) Startup() error {
	c.shutdown = make(chan struct{})
	return c.init()
}

func (c *Candidate) Shutdown() error {
	close(c.shutdown)
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
	return c.raft.AddVoter(config.ServerId, config.AdvertiseAddress(), 0, 5*time.Second).Error()
}

func (c *Candidate) RemoveMember(config *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.raft.RemoveServer(config.ServerId, 0, 5*time.Second).Error()
}

func (c *Candidate) TransferLeader() error {
	return c.raft.LeadershipTransfer().Error()
}

func (c *Candidate) RegisterEventHandler(handler EventHandler) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	handlerId := time.Now().UnixNano()
	c.handlers[handlerId] = handler
	return handlerId
}

func (c *Candidate) RemoveEventHandler(handlerId int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, handlerId)
}

func (c *Candidate) RunEventLoop() {
	for {
		select {
		case <-c.promote:
			c.mu.Lock()
			for _, handler := range c.handlers {
				if handler.OnPromote != nil {
					go handler.OnPromote()
				}
			}
			c.mu.Unlock()
		case <-c.demote:
			c.mu.Lock()
			for _, handler := range c.handlers {
				if handler.OnDemote != nil {
					go handler.OnDemote()
				}
			}
			c.mu.Unlock()
		case newLeader := <-c.newLeader:
			c.mu.Lock()
			for _, handler := range c.handlers {
				if handler.OnNewLeader != nil {
					go handler.OnNewLeader(newLeader)
				}
			}
			c.mu.Unlock()
		case <-c.shutdown:
			return
		}
	}
}

func NewCandidate(store *Store, config *Config, peers ...*Config) *Candidate {
	newLeader := make(chan raft.ServerAddress)
	c := &Candidate{
		store:     store,
		config:    config,
		rc:        PeersConfig(append(peers, config)),
		fsm:       NewLeaderTracker(newLeader),
		promote:   make(chan struct{}),
		demote:    make(chan struct{}),
		newLeader: newLeader,
		handlers:  make(map[int64]EventHandler),
	}
	go c.RunEventLoop()
	return c
}

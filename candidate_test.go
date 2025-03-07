package election

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/hashicorp/go-hclog"
)

var (
	c1        = &Config{ServerId: "c1", AdvertiseHostPort: "127.0.0.1:4401", ListenPort: 4401, LogLevel: hclog.Off}
	c2        = &Config{ServerId: "c2", AdvertiseHostPort: "127.0.0.1:4402", ListenPort: 4402, LogLevel: hclog.Off}
	c3        = &Config{ServerId: "c3", AdvertiseHostPort: "127.0.0.1:4403", ListenPort: 4403, LogLevel: hclog.Off}
	onPromote = func(conf *Config, c *Candidate) func() {
		return func() {
			log.Println(c.config.ServerId, ":", conf.ServerId, "become leader")
		}
	}
	onDemote = func(conf *Config, c *Candidate) func() {
		return func() {
			log.Println(c.config.ServerId, ":", conf.ServerId, "lose leader")
		}
	}
	onNewLeader = func(conf *Config, c *Candidate) func(address raft.ServerAddress) {
		return func(address raft.ServerAddress) {
			log.Println(c.config.ServerId, ":", "new leader address now is", address)
		}
	}
	startCandidate = func(config *Config, bootstrapCluster bool, peersConfig ...*Config) *Candidate {
		c := NewCandidate(NewMemoryStore(), config, peersConfig...)
		err := c.Startup()
		if err != nil {
			log.Println(err)
			return nil
		}
		if bootstrapCluster {
			c.BootstrapCluster()
		}
		return c
	}
)

func TestCandidate_Startup1(t *testing.T) {
	c := startCandidate(c1, false)
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c1, c),
		OnDemote:    onDemote(c1, c),
		OnNewLeader: onNewLeader(c1, c),
	})
}

func TestCandidate_Startup2(t *testing.T) {
	c := startCandidate(c2, false)
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c2, c),
		OnDemote:    onDemote(c2, c),
		OnNewLeader: onNewLeader(c2, c),
	})
}

func TestCandidate_BootstrapCluster(t *testing.T) {
	c := startCandidate(c3, true, c1, c2)
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c3, c),
		OnDemote:    onDemote(c3, c),
		OnNewLeader: onNewLeader(c3, c),
	})
}

func TestCandidate_AddMember(t *testing.T) {
	c := startCandidate(c3, true)
	go func() {
		for {
			if c.Leader() {
				log.Println(c.AddMember(c1))
				log.Println(c.AddMember(c2))
				break
			}
		}
	}()
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c3, c),
		OnDemote:    onDemote(c3, c),
		OnNewLeader: onNewLeader(c3, c),
	})
}

func TestCandidate_RemoveMember(t *testing.T) {
	c := startCandidate(c3, true, c1, c2)
	go func() {
		for {
			if c.Leader() {
				time.Sleep(5 * time.Second)
				log.Println(c.RemoveMember(c3))
				break
			}
		}
	}()
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c3, c),
		OnDemote:    onDemote(c3, c),
		OnNewLeader: onNewLeader(c3, c),
	})
}

func TestCandidate_RegisterOnNewLeaderReceiver(t *testing.T) {
	c := startCandidate(c3, true, c1, c2)
	go func() {
		time.Sleep(5 * time.Second)
		if err := c.TransferLeader(); err != nil {
			log.Println(err)
		}
	}()
	go func() {
		watcherId, ch := c.RegisterOnNewLeaderReceiver()
		defer c.DeregisterOnNewLeaderReceiver(watcherId)
		for address := range ch {
			fmt.Println("from receiver:", "new leader address now is", address)
		}
	}()
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c3, c),
		OnDemote:    onDemote(c3, c),
		OnNewLeader: onNewLeader(c3, c),
	})
}

func TestCandidate_Members(t *testing.T) {
	c := startCandidate(c3, true, c1, c2)
	err := c.Startup()
	if err != nil {
		t.Error(err)
		return
	}
	c.BootstrapCluster()
	leader, followers := c.Members()
	log.Println(leader.ID, leader.Address)
	for _, p := range followers {
		log.Println(p.ID, p.Address)
	}
}

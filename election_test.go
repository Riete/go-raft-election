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
			log.Println(conf.ServerId, "become leader", c.Leader())
		}
	}
	onDemote = func(c *Config) func() {
		return func() {
			log.Println(c.ServerId, "lose leader")
		}
	}
	onNewLeader = func(c *Config) func(address raft.ServerAddress) {
		return func(address raft.ServerAddress) {
			log.Println(c.ServerId, "new leader address now is", address)
		}
	}
)

func TestCandidate_C1(t *testing.T) {
	c := NewCandidate(NewMemoryStore(), c1, c2, c3)
	err := c.StartCluster()
	if err != nil {
		t.Error(err)
		return
	}
	watcherId, ch := c.RegisterWatcher()
	defer c.DeregisterWatcher(watcherId)
	go func() {
		for i := range ch {
			fmt.Println(i)
		}
	}()
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c1, c),
		OnDemote:    onDemote(c1),
		OnNewLeader: onNewLeader(c1),
	})
}

func TestCandidate_C2(t *testing.T) {
	c := NewCandidate(NewMemoryStore(), c2)
	err := c.Start()
	if err != nil {
		t.Error(err)
		return
	}
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c2, c),
		OnDemote:    onDemote(c2),
		OnNewLeader: onNewLeader(c2),
	})
}

func TestCandidate_C3(t *testing.T) {
	c := NewCandidate(NewMemoryStore(), c3)
	err := c.Start()
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("shutdown")
		_ = c.Shutdown()
		_ = c.Start()
		log.Println("start again")
	}()
	c.RunEventLoop(context.Background(), EventHandler{
		OnPromote:   onPromote(c3, c),
		OnDemote:    onDemote(c3),
		OnNewLeader: onNewLeader(c3),
	})
}

func TestPeersConfig(t *testing.T) {
	c := NewCandidate(NewMemoryStore(), c3, c1, c2)
	err := c.StartCluster()
	if err != nil {
		t.Error(err)
		return
	}
	for _, p := range c.Peers() {
		t.Log(p.ID, p.Address)
	}
}

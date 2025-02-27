package election

import (
	"log"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/hashicorp/go-hclog"
)

var (
	c1        = &Config{ServerId: "c1", AdvertiseHostPort: "c1:4401", ListenPort: 4401, LogLevel: hclog.Off}
	c2        = &Config{ServerId: "c2", AdvertiseHostPort: "c2:4402", ListenPort: 4402, LogLevel: hclog.Off}
	c3        = &Config{ServerId: "c3", AdvertiseHostPort: "c3:4403", ListenPort: 4403, LogLevel: hclog.Off}
	onPromote = func(c *Config) func() {
		return func() {
			log.Println(c.ServerId, "become leader")
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
	c, err := NewCandidate(NewMemoryStore(), c1, c2, c3)
	if err != nil {
		t.Error(err)
		return
	}
	c.BootstrapCluster()
	c.RunEventLoop(EventHandler{
		OnPromote:   onPromote(c1),
		OnDemote:    onDemote(c1),
		OnNewLeader: onNewLeader(c1),
	})
}

func TestCandidate_C2(t *testing.T) {
	c, err := NewCandidate(NewMemoryStore(), c2, c1, c3)
	if err != nil {
		t.Error(err)
		return
	}
	c.BootstrapCluster()
	c.RunEventLoop(EventHandler{
		OnPromote:   onPromote(c2),
		OnDemote:    onDemote(c2),
		OnNewLeader: onNewLeader(c2),
	})
}

func TestCandidate_C3(t *testing.T) {
	c, err := NewCandidate(NewMemoryStore(), c3, c1, c2)

	if err != nil {
		t.Error(err)
		return
	}
	c.BootstrapCluster()
	c.RunEventLoop(EventHandler{
		OnPromote:   onPromote(c3),
		OnDemote:    onDemote(c3),
		OnNewLeader: onNewLeader(c3),
	})
}

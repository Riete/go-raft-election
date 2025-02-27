package election

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	TransportDefaultMaxPool = 5
	TransportDefaultTimeout = 5 * time.Second
)

type Members map[raft.ServerID]raft.ServerAddress

type Config struct {
	ServerId          raft.ServerID
	BindIp            string
	ListenPort        int
	AdvertiseHostPort raft.ServerAddress
	TransportMaxPool  int
	TransportTimeout  time.Duration
	LogLevel          hclog.Level
}

func (c Config) BindAddr() string {
	return fmt.Sprintf("%s:%d", c.BindIp, c.ListenPort)
}

func (c Config) ServerAddress() raft.ServerAddress {
	return raft.ServerAddress(c.BindAddr())
}

func (c Config) AdvertiseAddress() raft.ServerAddress {
	if c.AdvertiseHostPort == "" {
		if c.BindIp == "" {
			return raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", c.ListenPort))
		}
		return c.ServerAddress()
	}
	return c.AdvertiseHostPort
}

func (c Config) AdvertiseAddr() (net.Addr, error) {
	return net.ResolveTCPAddr("tcp", string(c.AdvertiseAddress()))
}

func (c Config) RaftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = c.ServerId
	config.LogLevel = c.LogLevel.String()
	return config
}

func (c Config) ClusterMembersConfig(members Members) raft.Configuration {
	var config raft.Configuration
	for serverId, serverAddr := range members {
		config.Servers = append(config.Servers, raft.Server{ID: serverId, Address: serverAddr})
	}
	return config
}

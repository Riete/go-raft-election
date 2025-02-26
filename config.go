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
	ServerId         raft.ServerID
	BindIp           string
	Port             int
	TransportMaxPool int
	TransportTimeout time.Duration
}

func (c Config) BindAddr() string {
	return fmt.Sprintf("%s:%d", c.BindIp, c.Port)
}

func (c Config) ServerAddr() raft.ServerAddress {
	return raft.ServerAddress(c.BindAddr())
}

func (c Config) AdvertiseAddr() (net.Addr, error) {
	return net.ResolveTCPAddr("tcp", c.BindAddr())
}

func (c Config) RaftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = c.ServerId
	config.LogLevel = hclog.Error.String()
	return config
}

func (c Config) ClusterMembersConfig(members Members) raft.Configuration {
	var config raft.Configuration
	for serverId, serverAddr := range members {
		config.Servers = append(config.Servers, raft.Server{ID: serverId, Address: serverAddr})
	}
	return config
}

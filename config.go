package election

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	TransportDefaultMaxPool = 5
	TransportDefaultTimeout = 5 * time.Second
	DefaultLogLevel         = hclog.Error
)

type Config struct {
	ServerId          raft.ServerID
	BindIp            string
	ListenPort        int
	AdvertiseHostPort raft.ServerAddress
	TransportMaxPool  int
	TransportTimeout  time.Duration
	LogLevel          hclog.Level
	LogWriter         io.Writer
}

func (c *Config) SetDefault() {
	if c.TransportMaxPool == 0 {
		c.TransportMaxPool = TransportDefaultMaxPool
	}
	if c.TransportTimeout == 0 {
		c.TransportTimeout = TransportDefaultTimeout
	}
	if c.LogLevel == hclog.NoLevel {
		c.LogLevel = DefaultLogLevel
	}
}

func (c Config) BindAddress() string {
	return fmt.Sprintf("%s:%d", c.BindIp, c.ListenPort)
}

func (c Config) ServerAddress() raft.ServerAddress {
	return raft.ServerAddress(c.BindAddress())
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
	config.LogOutput = c.LogWriter
	return config
}

func PeersConfig(configs []*Config) raft.Configuration {
	var peersConfig raft.Configuration
	for _, config := range configs {
		peersConfig.Servers = append(
			peersConfig.Servers,
			raft.Server{
				ID:      config.ServerId,
				Address: config.AdvertiseAddress(),
			})
	}
	return peersConfig
}

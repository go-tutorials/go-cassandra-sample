package app

import (
	"github.com/core-go/core/server"
	mid "github.com/core-go/log/middleware"
	"github.com/core-go/log/zap"
)

type Config struct {
	Server     server.ServerConf `mapstructure:"server"`
	Cql        Cassandra         `mapstructure:"cassandra"`
	Log        log.Config        `mapstructure:"log"`
	MiddleWare mid.LogConfig     `mapstructure:"middleware"`
}

type Cassandra struct {
	PublicIp string `mapstructure:"public_ip"`
	UserName string `mapstructure:"user_name"`
	Password string `mapstructure:"password"`
}

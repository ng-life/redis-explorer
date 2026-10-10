package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/redis/go-redis/v9"
)

type Instance struct {
	Name       string   `toml:"name"`
	Mode       string   `toml:"mode"`
	Addresses  []string `toml:"addresses"`
	Database   int      `toml:"database"`
	MasterName string   `toml:"master_name"`
	Username   string   `toml:"username"`
	Password   string   `toml:"password"`
	TLS        bool     `toml:"tls"`
	ScanNodeID bool     `toml:"scan_node_id"`
}
type Config struct {
	Listen      string     `toml:"listen"`
	Instances   []Instance `toml:"instances"`
	AllowDelete bool       `toml:"allow_delete"`
	PageSize    int        `toml:"page_size"`
}
type redisFlags []string

func (v *redisFlags) String() string     { return strings.Join(*v, ",") }
func (v *redisFlags) Set(s string) error { *v = append(*v, s); return nil }
func loadConfig(args []string, output io.Writer) (Config, error) {
	c := Config{Listen: "127.0.0.1:8080", PageSize: 100}
	fs := flag.NewFlagSet("redis-explorer", flag.ContinueOnError)
	fs.SetOutput(output)
	path := fs.String("config", "config.toml", "TOML configuration file")
	fs.StringVar(path, "c", "config.toml", "TOML configuration file")
	listen := fs.String("listen", "", "Web server bind address")
	allow := fs.Bool("allow-delete", false, "Enable deletion of individual keys")
	disable := fs.Bool("disable-delete", false, "Disable deletion of individual keys")
	var instances redisFlags
	fs.Var(&instances, "redis", "Standalone instance NAME=URL (repeatable)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, fmt.Errorf("unexpected positional arguments")
	}
	if *allow && *disable {
		return c, fmt.Errorf("--allow-delete conflicts with --disable-delete")
	}
	if _, err := toml.DecodeFile(*path, &c); err != nil && !os.IsNotExist(err) {
		return c, err
	}
	if *listen != "" {
		c.Listen = *listen
	}
	if len(instances) > 0 {
		c.Instances = nil
		for _, s := range instances {
			name, url, ok := strings.Cut(s, "=")
			if !ok {
				url = s
			}
			c.Instances = append(c.Instances, Instance{Name: name, Mode: "standalone", Addresses: []string{url}})
		}
	}
	if *allow {
		c.AllowDelete = true
	}
	if *disable {
		c.AllowDelete = false
	}
	c.PageSize = max(10, min(500, c.PageSize))
	if _, err := net.ResolveTCPAddr("tcp", c.Listen); err != nil {
		return c, fmt.Errorf("invalid listen address: %w", err)
	}
	for n := range c.Instances {
		i := &c.Instances[n]
		if i.Mode == "" {
			i.Mode = "standalone"
		}
		if strings.TrimSpace(i.Name) == "" || len(i.Addresses) == 0 {
			return c, fmt.Errorf("instances[%d] requires a name and address", n)
		}
		if i.Mode != "standalone" && i.Mode != "sentinel" && i.Mode != "cluster" {
			return c, fmt.Errorf("instances[%d]: invalid mode", n)
		}
		if i.Database < 0 || i.Mode == "cluster" && i.Database != 0 {
			return c, fmt.Errorf("instances[%d]: invalid database", n)
		}
		if i.Mode == "sentinel" && strings.TrimSpace(i.MasterName) == "" {
			return c, fmt.Errorf("instances[%d] requires master_name", n)
		}
		for _, address := range i.Addresses {
			if _, err := redis.ParseURL(address); err != nil {
				return c, fmt.Errorf("instances[%d]: invalid Redis URL", n)
			}
		}
	}
	return c, nil
}

// Use RESP2 to retain the existing API's flat arrays for hashes and sorted sets.
func baseOptions(address string, db int) (*redis.Options, error) {
	o, err := redis.ParseURL(address)
	if err != nil {
		return nil, err
	}
	o.DB = db
	o.Protocol = 2
	o.ContextTimeoutEnabled = true
	o.DialTimeout = 5 * time.Second
	o.ReadTimeout = 5 * time.Second
	o.WriteTimeout = 5 * time.Second
	return o, nil
}
func openClient(ctx context.Context, i Instance, db int) (redis.UniversalClient, error) {
	if db < 0 || i.Mode == "cluster" && db != 0 {
		return nil, fmt.Errorf("invalid database")
	}
	o, err := baseOptions(i.Addresses[0], db)
	if err != nil {
		return nil, err
	}
	if i.Mode == "standalone" {
		return redis.NewClient(o), nil
	}
	if i.Mode == "sentinel" {
		var targetTLS *tls.Config
		if i.TLS {
			targetTLS = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		// Discover through each Sentinel URL separately, preserving its own credentials/TLS.
		// The discovered master is resolved on each HTTP request, so failover is picked up.
		return openSentinelMaster(ctx, i, db, targetTLS)
	}
	addresses := make([]string, 0, len(i.Addresses))
	for _, url := range i.Addresses {
		p, e := baseOptions(url, db)
		if e != nil {
			return nil, e
		}
		if p.Username != o.Username || p.Password != o.Password || (p.TLSConfig == nil) != (o.TLSConfig == nil) {
			return nil, fmt.Errorf("seed credentials and TLS must match")
		}
		addresses = append(addresses, p.Addr)
	}
	if i.Mode == "cluster" {
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: addresses, Username: o.Username, Password: o.Password, TLSConfig: o.TLSConfig, Protocol: 2, ContextTimeoutEnabled: true, DialTimeout: o.DialTimeout, ReadTimeout: o.ReadTimeout, WriteTimeout: o.WriteTimeout}), nil
	}
	return nil, fmt.Errorf("invalid Redis mode")
}

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func openSentinelMaster(parent context.Context, i Instance, db int, targetTLS *tls.Config) (redis.UniversalClient, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	for _, address := range i.Addresses {
		o, err := baseOptions(address, 0)
		if err != nil {
			return nil, err
		}
		sentinel := redis.NewSentinelClient(o)
		master, err := sentinel.GetMasterAddrByName(ctx, i.MasterName).Result()
		_ = sentinel.Close()
		if err != nil || len(master) != 2 {
			continue
		}
		return redis.NewClient(&redis.Options{Addr: net.JoinHostPort(master[0], master[1]), DB: db, Username: i.Username, Password: i.Password, TLSConfig: targetTLS, Protocol: 2, ContextTimeoutEnabled: true, DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}), nil
	}
	return nil, fmt.Errorf("Sentinel master discovery failed")
}

type scanNode struct {
	Host   string  `json:"host"`
	Port   int     `json:"port"`
	NodeID *string `json:"node_id"`
	Cursor uint64  `json:"cursor"`
}
type scanState struct {
	Nodes     []scanNode `json:"nodes"`
	Current   int        `json:"current"`
	UseNodeID bool       `json:"use_node_id"`
}

func discoverNodes(ctx context.Context, c redis.UniversalClient, ids bool) ([]scanNode, error) {
	nodes := map[string]scanNode{}
	if ids {
		text, err := c.Do(ctx, "CLUSTER", "NODES").Text()
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(text, "\n") {
			f := strings.Fields(line)
			if len(f) < 3 {
				continue
			}
			flags := "," + f[2] + ","
			if !strings.Contains(flags, ",master,") || strings.Contains(flags, ",fail,") || strings.Contains(flags, ",fail?,") || strings.Contains(flags, ",handshake,") || strings.Contains(flags, ",noaddr,") {
				continue
			}
			address := strings.Split(strings.Split(f[1], "@")[0], ",")[0]
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				continue
			}
			p, err := strconv.Atoi(port)
			if err != nil {
				continue
			}
			id := f[0]
			nodes[id] = scanNode{Host: host, Port: p, NodeID: &id}
		}
	} else {
		cluster, ok := c.(*redis.ClusterClient)
		if !ok {
			return nil, fmt.Errorf("expected cluster client")
		}
		slots, err := cluster.ClusterSlots(ctx).Result()
		if err != nil {
			return nil, err
		}
		for _, slot := range slots {
			if len(slot.Nodes) == 0 {
				continue
			}
			node := slot.Nodes[0]
			host, port, err := net.SplitHostPort(node.Addr)
			if err != nil {
				continue
			}
			p, err := strconv.Atoi(port)
			if err != nil {
				continue
			}
			nodes[node.Addr] = scanNode{Host: host, Port: p}
		}
	}
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]scanNode, 0, len(keys))
	for _, key := range keys {
		result = append(result, nodes[key])
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no primary nodes returned")
	}
	return result, nil
}
func scanKeys(ctx context.Context, c redis.UniversalClient, i Instance, state *scanState, pattern string, size int) ([]string, error) {
	keys := []string{}
	for rounds := 0; len(keys) < size && state.Current < len(state.Nodes) && rounds < 8; rounds++ {
		node := &state.Nodes[state.Current]
		target := c
		var direct *redis.Client
		if i.Mode == "cluster" {
			o, err := baseOptions(i.Addresses[0], 0)
			if err != nil {
				return nil, err
			}
			o.Addr = net.JoinHostPort(node.Host, strconv.Itoa(node.Port))
			direct = redis.NewClient(o)
			target = direct
		}
		args := []any{"SCAN", node.Cursor, "MATCH", pattern, "COUNT", size}
		if state.UseNodeID && node.NodeID != nil {
			args = append(args, *node.NodeID)
		}
		result, err := target.Do(ctx, args...).Slice()
		if direct != nil {
			_ = direct.Close()
		}
		if err != nil {
			return nil, err
		}
		if len(result) != 2 {
			return nil, fmt.Errorf("invalid SCAN response")
		}
		next, err := strconv.ParseUint(fmt.Sprint(result[0]), 10, 64)
		if err != nil {
			return nil, err
		}
		page, ok := result[1].([]any)
		if !ok {
			return nil, fmt.Errorf("invalid SCAN keys")
		}
		for _, v := range page {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("invalid SCAN key")
			}
			keys = append(keys, s)
		}
		node.Cursor = next
		if next == 0 {
			state.Current++
		}
	}
	return keys, nil
}
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune("*?[]\\", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

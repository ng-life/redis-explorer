package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"fmt"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"time"
)

type application struct{ config Config }
type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string { return e.message }
func bad(message string) error    { return &apiError{400, message} }

type redisOperationInput struct {
	Database            *int
	Prefix, View        string
	Cursor, Key, KeyB64 *string
}
type exportResult struct {
	Body                   []byte
	ContentType, Extension string
}

// runOperation is shared by Web and MCP; transport code never opens clients.
// Keep permission checks before connection and redact all raw Redis errors here.
func (a application) runOperation(ctx context.Context, id int, operation string, in redisOperationInput) (out any, err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(*apiError); !ok {
				slog.Warn("Redis request failed", "error_type", fmt.Sprintf("%T", err))
				err = &apiError{502, "Redis request failed; check address, credentials, TLS, and ACL permissions"}
			}
		}
	}()
	if id < 0 || id >= len(a.config.Instances) {
		return nil, &apiError{404, "Unknown Redis instance"}
	}
	switch operation {
	case "databases", "keys", "value", "export":
	case "delete":
		if !a.config.AllowDelete {
			return nil, &apiError{403, "Key deletion is disabled"}
		}
	default:
		return nil, bad("Unsupported Redis operation")
	}
	i := a.config.Instances[id]
	db := i.Database
	if in.Database != nil {
		db = *in.Database
	}
	if db < 0 {
		return nil, bad("Invalid database")
	}
	if i.Mode == "cluster" && db != 0 {
		return nil, bad("Redis Cluster only supports database 0")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := openClient(ctx, i, db)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	switch operation {
	case "databases":
		return a.listDatabases(ctx, i, client)
	case "keys":
		return a.listKeys(ctx, i, client, in)
	case "value":
		return a.readValue(ctx, client, in)
	case "export":
		return a.exportKey(ctx, client, in)
	default:
		return a.removeKey(ctx, client, in)
	}
}

func (a application) listDatabases(ctx context.Context, i Instance, c redis.UniversalClient) (any, error) {
	type entry struct {
		ID   int    `json:"id"`
		Keys *int64 `json:"keys"`
	}
	if i.Mode == "cluster" {
		return []entry{{ID: 0}}, nil
	}
	text, err := c.Info(ctx, "keyspace").Result()
	if err != nil {
		return nil, err
	}
	entries := map[int]*int64{i.Database: nil}
	for _, line := range strings.Split(text, "\n") {
		name, fields, ok := strings.Cut(line, ":")
		if !ok || !strings.HasPrefix(name, "db") {
			continue
		}
		id, err := strconv.Atoi(strings.TrimPrefix(name, "db"))
		if err != nil {
			continue
		}
		entries[id] = nil
		for _, field := range strings.Split(strings.TrimSpace(fields), ",") {
			if raw, ok := strings.CutPrefix(field, "keys="); ok {
				n, err := strconv.ParseInt(raw, 10, 64)
				if err == nil {
					entries[id] = &n
				}
			}
		}
	}
	ids := []int{}
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	result := []entry{}
	for _, id := range ids {
		result = append(result, entry{id, entries[id]})
	}
	return result, nil
}

type keyEntry struct {
	Name   string  `json:"name"`
	Key    *string `json:"key"`
	KeyB64 *string `json:"key_b64,omitempty"`
	IsDir  bool    `json:"is_dir"`
}

func buildEntries(keys []string, prefix, view string) []keyEntry {
	entries := map[string]keyEntry{}
	for _, key := range keys {
		remainder, ok := strings.CutPrefix(key, prefix)
		if !ok || remainder == "" {
			continue
		}
		e := keyEntry{}
		if view == "tree" {
			if name, _, ok := strings.Cut(remainder, ":"); ok {
				if name == "" {
					continue
				}
				if utf8.ValidString(name) {
					e.Name = name
					e.IsDir = true
					entries[name+"\x00dir"] = e
					continue
				}
			}
		}
		if utf8.ValidString(key) {
			e.Key = &key
			e.Name = remainder
			if view == "flat" {
				e.Name = key
			}
		} else {
			encoded := base64.RawURLEncoding.EncodeToString([]byte(key))
			e.KeyB64 = &encoded
			e.Name = "[binary key: " + encoded + "]"
		}
		entries[e.Name+"\x00file"] = e
	}
	result := make([]keyEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return !result[i].IsDir && result[j].IsDir
		}
		return result[i].Name < result[j].Name
	})
	return result
}
func (a application) listKeys(ctx context.Context, i Instance, c redis.UniversalClient, in redisOperationInput) (any, error) {
	prefix := in.Prefix
	if strings.ContainsRune(prefix, 0) {
		return nil, bad("Invalid key prefix")
	}
	view := "tree"
	if in.View == "flat" {
		view = "flat"
	}
	state := scanState{Nodes: []scanNode{{}}, UseNodeID: i.ScanNodeID}
	if in.Cursor != nil {
		data, err := base64.RawURLEncoding.DecodeString(*in.Cursor)
		if err != nil || json.Unmarshal(data, &state) != nil || state.Current < 0 || state.Current > len(state.Nodes) {
			return nil, bad("Invalid scan cursor")
		}
		// A cursor may only reference current configured primaries. Never trust client-supplied hosts.
		if i.Mode == "cluster" {
			known, err := discoverNodes(ctx, c, i.ScanNodeID)
			if err != nil {
				return nil, err
			}
			for _, node := range state.Nodes {
				found := false
				for _, k := range known {
					if node.Host == k.Host && node.Port == k.Port && (!i.ScanNodeID || node.NodeID != nil && k.NodeID != nil && *node.NodeID == *k.NodeID) {
						found = true
						break
					}
				}
				if !found {
					return nil, bad("Scan topology changed; restart browsing")
				}
			}
		} else if len(state.Nodes) != 1 || state.Nodes[0].Host != "" || state.Nodes[0].Port != 0 {
			return nil, bad("Invalid scan cursor")
		}
		state.UseNodeID = i.ScanNodeID
	} else if i.Mode == "cluster" {
		nodes, err := discoverNodes(ctx, c, i.ScanNodeID)
		if err != nil {
			return nil, err
		}
		state.Nodes = nodes
	}
	keys, err := scanKeys(ctx, c, i, &state, globEscape(prefix)+"*", a.config.PageSize)
	if err != nil {
		return nil, err
	}
	var cursor *string
	if state.Current < len(state.Nodes) {
		data, _ := json.Marshal(state)
		s := base64.RawURLEncoding.EncodeToString(data)
		cursor = &s
	}
	return map[string]any{"prefix": prefix, "view": view, "nodes": buildEntries(keys, prefix, view), "cursor": cursor, "scanned": len(keys)}, nil
}
func operationKey(in redisOperationInput) (string, string, error) {
	if in.KeyB64 != nil {
		b, err := base64.RawURLEncoding.DecodeString(*in.KeyB64)
		if err != nil {
			return "", "", bad("Invalid encoded key")
		}
		return string(b), "base64:" + *in.KeyB64, nil
	}
	if in.Key != nil {
		return *in.Key, *in.Key, nil
	}
	return "", "", bad("Missing key")
}
func (a application) removeKey(ctx context.Context, c redis.UniversalClient, in redisOperationInput) (any, error) {
	key, _, err := operationKey(in)
	if err != nil {
		return nil, err
	}
	n, err := c.Unlink(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return map[string]bool{"deleted": n > 0}, nil
}

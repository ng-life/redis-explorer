package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

type application struct{ config Config }
type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string { return e.message }
func bad(message string) error    { return &apiError{400, message} }
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func newHandler(c Config) http.Handler {
	a := application{c}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"allow_delete": c.AllowDelete, "page_size": c.PageSize})
	})
	mux.HandleFunc("GET /api/instances", func(w http.ResponseWriter, r *http.Request) {
		items := []any{}
		for id, i := range c.Instances {
			items = append(items, map[string]any{"id": id, "name": i.Name, "mode": i.Mode})
		}
		writeJSON(w, items)
	})
	for _, route := range []struct {
		pattern string
		fn      func(http.ResponseWriter, *http.Request, Instance, redis.UniversalClient) error
	}{
		{"GET /api/instances/{id}/databases", a.databases}, {"GET /api/instances/{id}/keys", a.keys}, {"DELETE /api/instances/{id}/keys", a.deleteKey}, {"GET /api/instances/{id}/value", a.value}, {"GET /api/instances/{id}/download", a.download},
	} {
		mux.HandleFunc(route.pattern, a.wrap(route.fn))
	}
	return mux
}
func (a application) wrap(fn func(http.ResponseWriter, *http.Request, Instance, redis.UniversalClient) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := func() error {
			id, err := strconv.Atoi(r.PathValue("id"))
			if err != nil || id < 0 || id >= len(a.config.Instances) {
				return &apiError{404, "Unknown Redis instance"}
			}
			// Reject disabled writes before opening a Redis connection.
			if r.Method == http.MethodDelete && !a.config.AllowDelete {
				return &apiError{403, "Key deletion is disabled"}
			}
			i := a.config.Instances[id]
			db := i.Database
			if raw, ok := r.URL.Query()["db"]; ok {
				db, err = strconv.Atoi(raw[0])
				if err != nil || db < 0 {
					return bad("Invalid database")
				}
			}
			if i.Mode == "cluster" && db != 0 {
				return bad("Redis Cluster only supports database 0")
			}
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
			c, err := openClient(ctx, i, db)
			if err != nil {
				return err
			}
			defer c.Close()
			return fn(w, r, i, c)
		}()
		if err != nil {
			if e, ok := err.(*apiError); ok {
				http.Error(w, e.message, e.status)
			} else {
				slog.Warn("Redis request failed", "error_type", fmt.Sprintf("%T", err))
				http.Error(w, "Redis request failed; check address, credentials, TLS, and ACL permissions", 502)
			}
		}
	}
}
func (a application) databases(w http.ResponseWriter, r *http.Request, i Instance, c redis.UniversalClient) error {
	type entry struct {
		ID   int    `json:"id"`
		Keys *int64 `json:"keys"`
	}
	if i.Mode == "cluster" {
		writeJSON(w, []entry{{ID: 0}})
		return nil
	}
	text, err := c.Info(r.Context(), "keyspace").Result()
	if err != nil {
		return err
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
	writeJSON(w, result)
	return nil
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
func (a application) keys(w http.ResponseWriter, r *http.Request, i Instance, c redis.UniversalClient) error {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	if strings.ContainsRune(prefix, 0) {
		return bad("Invalid key prefix")
	}
	view := "tree"
	if q.Get("view") == "flat" {
		view = "flat"
	}
	state := scanState{Nodes: []scanNode{{}}, UseNodeID: i.ScanNodeID}
	if raw, ok := q["cursor"]; ok {
		data, err := base64.RawURLEncoding.DecodeString(raw[0])
		if err != nil || json.Unmarshal(data, &state) != nil || state.Current < 0 || state.Current > len(state.Nodes) {
			return bad("Invalid scan cursor")
		}
		// A cursor may only reference current configured primaries. Never trust client-supplied hosts.
		if i.Mode == "cluster" {
			known, err := discoverNodes(r.Context(), c, i.ScanNodeID)
			if err != nil {
				return err
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
					return bad("Scan topology changed; restart browsing")
				}
			}
		} else if len(state.Nodes) != 1 || state.Nodes[0].Host != "" || state.Nodes[0].Port != 0 {
			return bad("Invalid scan cursor")
		}
		state.UseNodeID = i.ScanNodeID
	} else if i.Mode == "cluster" {
		nodes, err := discoverNodes(r.Context(), c, i.ScanNodeID)
		if err != nil {
			return err
		}
		state.Nodes = nodes
	}
	keys, err := scanKeys(r.Context(), c, i, &state, globEscape(prefix)+"*", a.config.PageSize)
	if err != nil {
		return err
	}
	var cursor *string
	if state.Current < len(state.Nodes) {
		data, _ := json.Marshal(state)
		s := base64.RawURLEncoding.EncodeToString(data)
		cursor = &s
	}
	writeJSON(w, map[string]any{"prefix": prefix, "view": view, "nodes": buildEntries(keys, prefix, view), "cursor": cursor, "scanned": len(keys)})
	return nil
}
func queryKey(q url.Values) (string, string, error) {
	if raw, ok := q["key_b64"]; ok {
		b, err := base64.RawURLEncoding.DecodeString(raw[0])
		if err != nil {
			return "", "", bad("Invalid encoded key")
		}
		return string(b), "base64:" + raw[0], nil
	}
	if raw, ok := q["key"]; ok {
		return raw[0], raw[0], nil
	}
	return "", "", bad("Missing key")
}
func (a application) deleteKey(w http.ResponseWriter, r *http.Request, i Instance, c redis.UniversalClient) error {
	key, _, err := queryKey(r.URL.Query())
	if err != nil {
		return err
	}
	n, err := c.Unlink(r.Context(), key).Result()
	if err != nil {
		return err
	}
	writeJSON(w, map[string]bool{"deleted": n > 0})
	return nil
}

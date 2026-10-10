package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/http"
	"strconv"
	"unicode/utf8"
)

// Redis strings contain arbitrary bytes; JSON must never replace invalid UTF-8.
func redisJSON(value any) any {
	switch v := value.(type) {
	case string:
		if utf8.ValidString(v) {
			return map[string]any{"text": v, "encoding": "utf-8"}
		}
		return map[string]any{"base64": base64.StdEncoding.EncodeToString([]byte(v)), "encoding": "base64"}
	case []any:
		out := make([]any, len(v))
		for n, item := range v {
			out[n] = redisJSON(item)
		}
		return out
	default:
		return v
	}
}
func (a application) value(w http.ResponseWriter, r *http.Request, i Instance, c redis.UniversalClient) error {
	key, display, err := queryKey(r.URL.Query())
	if err != nil {
		return err
	}
	ctx := r.Context()
	kind, err := c.Type(ctx, key).Result()
	if err != nil {
		return err
	}
	if kind == "none" {
		return &apiError{404, "Key no longer exists"}
	}
	ttl, err := c.Do(ctx, "PTTL", key).Int64()
	if err != nil {
		return err
	}
	raw := r.URL.Query().Get("cursor")
	offset := int64(0)
	if raw != "" && kind != "stream" {
		offset, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || offset < 0 {
			return bad("Invalid value cursor")
		}
	}
	size := int64(a.config.PageSize)
	var total int64
	var cursor *string
	var items any
	nextOffset := func() {
		if next := offset + size; next < total {
			s := strconv.FormatInt(next, 10)
			cursor = &s
		}
	}
	countCmd := ""
	switch kind {
	case "string":
		countCmd = "STRLEN"
	case "hash":
		countCmd = "HLEN"
	case "list":
		countCmd = "LLEN"
	case "set":
		countCmd = "SCARD"
	case "zset":
		countCmd = "ZCARD"
	case "stream":
		countCmd = "XLEN"
	default:
		return &apiError{502, "Unsupported Redis key type: " + kind}
	}
	total, err = c.Do(ctx, countCmd, key).Int64()
	if err != nil {
		return err
	}
	switch kind {
	case "string":
		v, e := c.Do(ctx, "GETRANGE", key, offset, offset+size-1).Result()
		err = e
		items = redisJSON(v)
		nextOffset()
	case "list", "zset":
		args := []any{"LRANGE", key, offset, offset + size - 1}
		if kind == "zset" {
			args[0] = "ZRANGE"
			args = append(args, "WITHSCORES")
		}
		values, e := c.Do(ctx, args...).Slice()
		err = e
		if kind == "zset" {
			items = paired(values, "member", "score")
		} else {
			items = redisJSON(values)
		}
		nextOffset()
	case "hash", "set":
		cmd := "HSCAN"
		if kind == "set" {
			cmd = "SSCAN"
		}
		values, e := c.Do(ctx, cmd, key, offset, "COUNT", size).Slice()
		if e != nil {
			return e
		}
		if len(values) != 2 {
			return fmt.Errorf("invalid collection scan response")
		}
		next := fmt.Sprint(values[0])
		if next != "0" {
			cursor = &next
		}
		page, ok := values[1].([]any)
		if !ok {
			return fmt.Errorf("invalid collection items")
		}
		if kind == "hash" {
			items = paired(page, "field", "value")
		} else {
			items = redisJSON(page)
		}
	case "stream":
		start := "-"
		if raw != "" {
			start = "(" + raw
		}
		values, e := c.Do(ctx, "XRANGE", key, start, "+", "COUNT", size).Slice()
		err = e
		items = redisJSON(values)
		if len(values) == int(size) {
			if entry, ok := values[len(values)-1].([]any); ok && len(entry) > 0 {
				s := fmt.Sprint(entry[0])
				cursor = &s
			}
		}
	}
	if err != nil {
		return err
	}
	writeJSON(w, map[string]any{"key": display, "kind": kind, "ttl_ms": ttl, "total": total, "cursor": cursor, "items": items})
	return nil
}
func paired(values []any, a, b string) []any {
	out := []any{}
	for n := 0; n+1 < len(values); n += 2 {
		out = append(out, map[string]any{a: redisJSON(values[n]), b: redisJSON(values[n+1])})
	}
	return out
}
func (a application) download(w http.ResponseWriter, r *http.Request, i Instance, c redis.UniversalClient) error {
	key, display, err := queryKey(r.URL.Query())
	if err != nil {
		return err
	}
	ctx := r.Context()
	kind, err := c.Type(ctx, key).Result()
	if err != nil {
		return err
	}
	if kind == "none" {
		return &apiError{404, "Key no longer exists"}
	}
	var body []byte
	contentType, extension := "application/json", "json"
	if kind == "string" {
		body, err = c.Get(ctx, key).Bytes()
		contentType = "application/octet-stream"
		extension = "bin"
	} else {
		var args []any
		switch kind {
		case "hash":
			args = []any{"HGETALL", key}
		case "list":
			args = []any{"LRANGE", key, 0, 9999}
		case "set":
			args = []any{"SMEMBERS", key}
		case "zset":
			args = []any{"ZRANGE", key, 0, 9999, "WITHSCORES"}
		case "stream":
			args = []any{"XRANGE", key, "-", "+", "COUNT", 1000}
		default:
			return &apiError{502, "Unsupported Redis key type: " + kind}
		}
		value, e := c.Do(ctx, args...).Result()
		if e != nil {
			return e
		}
		body, err = json.MarshalIndent(map[string]any{"key": display, "type": kind, "items": redisJSON(value), "note": "Lists and sorted sets are limited to 10000 entries; streams to 1000 entries per download. Hashes and sets are exported in full."}, "", "  ")
	}
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", "attachment; filename=\"redis-key."+extension+"\"")
	_, err = w.Write(body)
	return err
}

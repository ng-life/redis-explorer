package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func connectMCP(t *testing.T, h http.Handler) (context.Context, *mcp.ClientSession) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return ctx, session
}
func callMCP(t *testing.T, ctx context.Context, s *mcp.ClientSession, name string, args map[string]any, wantError bool) string {
	t.Helper()
	out, err := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		if wantError {
			return err.Error()
		}
		t.Fatal(name, err)
	}
	if out.IsError != wantError {
		t.Fatalf("%s: %+v", name, out)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-password") {
		t.Fatal("credentials leaked", string(raw))
	}
	return string(raw)
}
func TestMCPConnectionSelection(t *testing.T) {
	for _, names := range [][]string{{}, {"a", "b"}, {"a", "a"}} {
		name := ""
		if len(names) > 0 && names[0] == names[len(names)-1] {
			name = names[0]
		}
		if _, err := connectionIndex(names, name); err == nil {
			t.Fatal("ambiguous connection accepted", names)
		}
	}
	if id, err := connectionIndex([]string{"a"}, ""); err != nil || id != 0 {
		t.Fatal(id, err)
	}
	if id, err := connectionIndex([]string{"a", "b"}, "b"); err != nil || id != 1 {
		t.Fatal(id, err)
	}
}
func testMCPGuards(t *testing.T, h http.Handler) {
	t.Helper()
	for _, tc := range []struct {
		host, origin string
		want         int
	}{
		{"evil.example:1234", "", 403}, {"127.0.0.1:1234", "null", 403},
		{"127.0.0.1:1234", "https://evil.example", 403}, {"127.0.0.1:1234", "http://127.0.0.1:1234", 405},
		{"localhost:1234", "", 405},
	} {
		req := httptest.NewRequest("GET", "http://"+tc.host+"/mcp", nil)
		req.Header.Set("Accept", "text/event-stream")
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
	req := httptest.NewRequest("POST", "http://127.0.0.1:1234/mcp", strings.NewReader(strings.Repeat("x", 1024*1024+1)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 413 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestMCPOfflineAndDeletePolicy(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:0", Instances: []Instance{{Name: "offline", Mode: "standalone", Addresses: []string{"redis://:private-password@127.0.0.1:1"}}}, PageSize: 10}
	ctx, s := connectMCP(t, newHandler(cfg))
	tools, err := s.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 5 {
		t.Fatal(tools, err)
	}
	for _, tool := range tools.Tools {
		if !tool.Annotations.ReadOnlyHint || tool.Name == "delete_key" {
			t.Fatal(tool)
		}
	}
	callMCP(t, ctx, s, "list_connections", map[string]any{}, false)
	for _, args := range []map[string]any{{}, {"key": "a", "key_b64": "YQ"}, {"key": "a", "connection": "missing"}, {"key": "a", "database": -1}, {"key": "a", "view": "bad"}} {
		callMCP(t, ctx, s, "get_value", args, true)
	}
	if _, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "delete_key", Arguments: map[string]any{"key": "a"}}); err == nil {
		t.Fatal("disabled tool registered")
	}
	if _, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "get_value", Arguments: map[string]any{"key": "a", "allow_delete": true}}); err == nil {
		t.Fatal("unknown write flag accepted")
	}
}
func TestMCPRedisOperations(t *testing.T) {
	port := startRedis(t, "", false)
	i := Instance{Name: "local", Mode: "standalone", Addresses: []string{fmt.Sprintf("redis://127.0.0.1:%d", port)}, Database: 2}
	client, err := openClient(context.Background(), i, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	for _, cmd := range [][]any{{"SET", "page", strings.Repeat("x", 25)}, {"SET", string([]byte{255, 0, 1}), string([]byte{255, 0, 2})}, {"SET", "", "empty"}, {"RPUSH", "list", "one", "two"}, {"HSET", "hash", "f", "v"}, {"SADD", "set", "v"}, {"ZADD", "zset", 1, "v"}, {"XADD", "stream", "*", "f", "v"}} {
		if err := client.Do(ctx, cmd...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{Listen: "127.0.0.1:0", Instances: []Instance{i}, PageSize: 10, AllowDelete: true}
	ctx, s := connectMCP(t, newHandler(cfg))
	tools, err := s.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 6 {
		t.Fatal(tools, err)
	}
	for _, tool := range tools.Tools {
		if tool.Name == "delete_key" && (tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint) {
			t.Fatal("deletion marked read-only")
		}
	}
	callMCP(t, ctx, s, "list_databases", map[string]any{}, false)
	callMCP(t, ctx, s, "list_keys", map[string]any{"view": "flat"}, false)
	for _, key := range []string{"page", "", "list", "hash", "set", "zset", "stream"} {
		callMCP(t, ctx, s, "get_value", map[string]any{"key": key}, false)
		callMCP(t, ctx, s, "export_key", map[string]any{"key": key}, false)
	}
	raw := callMCP(t, ctx, s, "get_value", map[string]any{"key": "page"}, false)
	if !strings.Contains(raw, `"cursor":"10"`) {
		t.Fatal(raw)
	}
	raw = callMCP(t, ctx, s, "get_value", map[string]any{"key": "page", "cursor": "20"}, false)
	if !strings.Contains(raw, "xxxxx") || !strings.Contains(raw, `"cursor":null`) {
		t.Fatal(raw)
	}

	webResult := request(t, newHandler(cfg), "GET", "/api/instances/0/value?key=page", 200)
	mcpResult, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "get_value", Arguments: map[string]any{"key": "page"}})
	if err != nil || mcpResult.IsError {
		t.Fatal(mcpResult, err)
	}
	encoded, _ := json.Marshal(mcpResult.StructuredContent)
	var structured struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(encoded, &structured); err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(object(t, webResult))
	got, _ := json.Marshal(object(t, structured.Result))
	if string(want) != string(got) {
		t.Fatalf("Web/MCP result mismatch: %s != %s", want, got)
	}
	key := base64.RawURLEncoding.EncodeToString([]byte{255, 0, 1})
	raw = callMCP(t, ctx, s, "get_value", map[string]any{"key_b64": key}, false)
	if !strings.Contains(raw, "/wAC") {
		t.Fatal(raw)
	}
	raw = callMCP(t, ctx, s, "export_key", map[string]any{"key_b64": key}, false)
	if !strings.Contains(raw, "/wAC") {
		t.Fatal(raw)
	}
	callMCP(t, ctx, s, "get_value", map[string]any{"key_b64": "!"}, true)
	callMCP(t, ctx, s, "list_keys", map[string]any{"cursor": "bad"}, true)
	callMCP(t, ctx, s, "get_value", map[string]any{"key": "page", "cursor": "-1"}, true)
	callMCP(t, ctx, s, "delete_key", map[string]any{"key_b64": key}, false)
	if client.Exists(ctx, string([]byte{255, 0, 1})).Val() != 0 {
		t.Fatal("delete did not apply")
	}
	callMCP(t, ctx, s, "get_value", map[string]any{"key_b64": key}, true)
}
func TestMCPGuards(t *testing.T) { testMCPGuards(t, newHandler(Config{Listen: "127.0.0.1:1234"})) }

func TestSharedOperationDeleteGuard(t *testing.T) {
	// An invalid URL would fail if a connection were opened. Disabled deletion
	// must be rejected by the business layer even when no MCP tool is registered.
	a := application{config: Config{Instances: []Instance{{Name: "offline", Addresses: []string{"invalid"}}}}}
	key := "target"
	_, err := a.runOperation(context.Background(), 0, "delete", redisOperationInput{Key: &key})
	if e, ok := err.(*apiError); !ok || e.status != 403 {
		t.Fatalf("delete guard bypassed: %v", err)
	}
}

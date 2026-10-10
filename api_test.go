package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
func startRedis(t *testing.T, extra string, sentinel bool) int {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server required for integration tests")
	}
	port := freePort(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "redis.conf")
	if strings.Contains(extra, "cluster-enabled yes") {
		extra += fmt.Sprintf("\ncluster-port %d", freePort(t))
	}
	config := fmt.Sprintf("bind 127.0.0.1\nport %d\nsave \"\"\nappendonly no\ndir %s\n%s\n", port, dir, extra)
	if err = os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{path}
	if sentinel {
		args = append(args, "--sentinel")
	}
	cmd := exec.Command(binary, args...)
	logFile, err := os.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Signal(os.Interrupt); _ = cmd.Wait(); _ = logFile.Close() })
	c := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.Ping(context.Background()).Err() == nil {
			return port
		}
		time.Sleep(20 * time.Millisecond)
	}
	logs, _ := os.ReadFile(filepath.Join(dir, "log"))
	t.Fatalf("Redis did not start: %s", logs)
	return 0
}
func request(t *testing.T, h http.Handler, method, path string, status int) []byte {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	if w.Code != status {
		t.Fatalf("%s %s: status %d: %s", method, path, w.Code, w.Body.String())
	}
	return w.Body.Bytes()
}
func object(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestStandaloneAPI(t *testing.T) {
	port := startRedis(t, "", false)
	address := fmt.Sprintf("redis://127.0.0.1:%d", port)
	i := Instance{Name: "test", Mode: "standalone", Addresses: []string{address}, Database: 2}
	c, err := openClient(context.Background(), i, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	commands := [][]any{{"SET", "group:string", strings.Repeat("x", 25)}, {"HSET", "group:hash", "a", "one", "b", "two"}, {"RPUSH", "group:list", "one", "two"}, {"SADD", "group:set", "one", "two"}, {"ZADD", "group:zset", 1, "one", 2, "two"}, {"XADD", "group:stream", "*", "a", "one"}, {"SET", string([]byte{255, 0, 1}), string([]byte{255, 0, 2})}, {"SET", "literal[*]:key", "literal"}, {"PEXPIRE", "group:hash", 60000}}
	for _, cmd := range commands {
		if err = c.Do(ctx, cmd...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{Instances: []Instance{i}, PageSize: 10}
	h := newHandler(cfg)
	request(t, h, "GET", "/", 200)
	instances := string(request(t, h, "GET", "/api/instances", 200))
	if strings.Contains(instances, "redis://") {
		t.Fatal("address leaked")
	}
	request(t, h, "GET", "/api/settings", 200)
	dbs := string(request(t, h, "GET", "/api/instances/0/databases", 200))
	if !strings.Contains(dbs, `"id":2`) {
		t.Fatal(dbs)
	}
	tree := object(t, request(t, h, "GET", "/api/instances/0/keys?prefix=group:", 200))
	if len(tree["nodes"].([]any)) != 6 {
		t.Fatal(tree)
	}
	literal := object(t, request(t, h, "GET", "/api/instances/0/keys?prefix=literal%5B%2A%5D:", 200))
	if len(literal["nodes"].([]any)) != 1 {
		t.Fatal(literal)
	}
	for _, kind := range []string{"string", "hash", "list", "set", "zset", "stream"} {
		t.Run(kind, func(t *testing.T) {
			v := object(t, request(t, h, "GET", "/api/instances/0/value?key=group:"+kind, 200))
			if v["kind"] != kind {
				t.Fatal(v)
			}
			download := request(t, h, "GET", "/api/instances/0/download?key=group:"+kind, 200)
			if kind == "string" {
				if string(download) != strings.Repeat("x", 25) {
					t.Fatal("string download truncated")
				}
				next := object(t, request(t, h, "GET", "/api/instances/0/value?key=group:string&cursor=20", 200))
				if next["cursor"] != nil || next["items"].(map[string]any)["text"] != "xxxxx" {
					t.Fatal(next)
				}
			} else {
				object(t, download)
			}
		})
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte{255, 0, 1})
	binary := object(t, request(t, h, "GET", "/api/instances/0/value?key_b64="+encoded, 200))
	if binary["items"].(map[string]any)["base64"] != "/wAC" {
		t.Fatal(binary)
	}
	if string(request(t, h, "GET", "/api/instances/0/download?key_b64="+encoded, 200)) != string([]byte{255, 0, 2}) {
		t.Fatal("binary download changed")
	}
	flat := object(t, request(t, h, "GET", "/api/instances/0/keys?view=flat", 200))
	found := false
	for _, e := range flat["nodes"].([]any) {
		if e.(map[string]any)["key_b64"] == encoded {
			found = true
		}
	}
	if !found {
		t.Fatal("binary key omitted")
	}
	request(t, h, "DELETE", "/api/instances/0/keys?key=group:string", 403)
	if c.Exists(ctx, "group:string").Val() != 1 {
		t.Fatal("disabled delete wrote")
	}
	cfg.AllowDelete = true
	h = newHandler(cfg)
	v := object(t, request(t, h, "DELETE", "/api/instances/0/keys?key_b64="+encoded, 200))
	if v["deleted"] != true {
		t.Fatal(v)
	}
	request(t, h, "GET", "/api/instances/0/value?key_b64="+encoded, 404)
	for _, path := range []string{"keys?cursor=bad", "keys?prefix=%00", "value?key_b64=!", "value?key=group:list&cursor=-1", "value", "value?db=-1&key=a"} {
		request(t, h, "GET", "/api/instances/0/"+path, 400)
	}
	request(t, h, "GET", "/api/instances/5/keys", 404)
}
func TestSentinelAPI(t *testing.T) {
	master := startRedis(t, "", false)
	sentinel := startRedis(t, fmt.Sprintf("sentinel monitor test 127.0.0.1 %d 1", master), true)
	i := Instance{Name: "sentinel", Mode: "sentinel", MasterName: "test", Addresses: []string{fmt.Sprintf("redis://127.0.0.1:%d", sentinel)}, Database: 3}
	c, err := openClient(context.Background(), i, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Set(context.Background(), "sentinel:test", "ok", 0).Err(); err != nil {
		t.Fatal(err)
	}
	h := newHandler(Config{Instances: []Instance{i}, PageSize: 10})
	v := object(t, request(t, h, "GET", "/api/instances/0/value?key=sentinel:test", 200))
	if v["items"].(map[string]any)["text"] != "ok" {
		t.Fatal(v)
	}
}
func TestClusterAPI(t *testing.T) {
	if _, err := exec.LookPath("redis-cli"); err != nil {
		t.Skip("redis-cli required")
	}
	addresses := []string{}
	urls := []string{}
	for n := 0; n < 3; n++ {
		port := startRedis(t, "cluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 1000", false)
		addresses = append(addresses, fmt.Sprintf("127.0.0.1:%d", port))
		urls = append(urls, "redis://"+addresses[n])
	}
	args := append([]string{"--cluster", "create"}, addresses...)
	args = append(args, "--cluster-yes")
	if output, err := exec.Command("redis-cli", args...).CombinedOutput(); err != nil {
		t.Fatalf("create cluster: %v %s", err, output)
	}
	i := Instance{Name: "cluster", Mode: "cluster", Addresses: urls}
	c, err := openClient(context.Background(), i, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = c.Set(ctx, "ready", "ok", 0).Err()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for n := 0; n < 150; n++ {
		if err = c.Set(ctx, "test:"+strconv.Itoa(n), "value", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	h := newHandler(Config{Instances: []Instance{i}, PageSize: 10})
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 100; page++ {
		path := "/api/instances/0/keys?view=flat&prefix=test:"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		v := object(t, request(t, h, "GET", path, 200))
		for _, entry := range v["nodes"].([]any) {
			seen[entry.(map[string]any)["key"].(string)] = true
		}
		if v["cursor"] == nil {
			break
		}
		cursor = v["cursor"].(string)
	}
	if len(seen) != 150 {
		t.Fatalf("cluster scan returned %d keys", len(seen))
	}
	request(t, h, "GET", "/api/instances/0/value?key=test:1", 200)
	request(t, h, "GET", "/api/instances/0/keys?db=1", 400)
	request(t, h, "GET", "/api/instances/0/databases", 200)
	state := scanState{Nodes: []scanNode{{Host: "127.0.0.1", Port: 1}}, Current: 0}
	data, _ := json.Marshal(state)
	request(t, h, "GET", "/api/instances/0/keys?cursor="+base64.RawURLEncoding.EncodeToString(data), 400)
	nodes, err := discoverNodes(ctx, c, true)
	if err != nil || len(nodes) != 3 {
		t.Fatalf("node IDs: %v %v", nodes, err)
	}
}
func TestConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	err := os.WriteFile(path, []byte("page_size=900\nallow_delete=true\n[[instances]]\nname='example'\naddresses=['redis://127.0.0.1:6379']\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	c, err := loadConfig([]string{"--config", path, "--disable-delete", "--listen", "127.0.0.1:9000", "--redis", "a=redis://127.0.0.1:6379", "--redis", "b=redis://127.0.0.1:6380"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.PageSize != 500 || c.AllowDelete || len(c.Instances) != 2 || c.Listen != "127.0.0.1:9000" {
		t.Fatal(c)
	}
	for _, args := range [][]string{{"--allow-delete", "--disable-delete"}, {"--redis", "a=bad"}, {"--listen", "bad"}, {"unused"}} {
		if _, err := loadConfig(args, io.Discard); err == nil {
			t.Fatal(args)
		}
	}
}

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// A tiny RESP proxy fixture verifies the vendor SCAN extension without requiring
// a Tencent Cloud instance. Standard Redis correctly rejects that extra argument.
func TestScanNodeIDExtension(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	observed := make(chan []string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if len(line) < 2 || line[0] != '*' {
				return
			}
			count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
			if err != nil {
				return
			}
			args := make([]string, count)
			for n := range args {
				line, err = reader.ReadString('\n')
				if err != nil {
					return
				}
				length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
				if err != nil || length < 0 {
					return
				}
				data := make([]byte, length+2)
				if _, err = io.ReadFull(reader, data); err != nil {
					return
				}
				args[n] = string(data[:length])
			}
			switch strings.ToUpper(args[0]) {
			case "HELLO":
				_, _ = io.WriteString(conn, "-ERR unknown command\r\n")
			case "CLIENT":
				_, _ = io.WriteString(conn, "+OK\r\n")
			case "SCAN":
				observed <- args
				_, _ = io.WriteString(conn, "*2\r\n$1\r\n0\r\n*1\r\n$3\r\nfoo\r\n")
			default:
				_, _ = io.WriteString(conn, "+OK\r\n")
			}
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	p, _ := strconv.Atoi(port)
	id := "primary-node-id"
	instance := Instance{Mode: "cluster", Addresses: []string{"redis://" + listener.Addr().String()}, ScanNodeID: true}
	state := scanState{Nodes: []scanNode{{Host: host, Port: p, NodeID: &id}}, UseNodeID: true}
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), Protocol: 2})
	defer client.Close()
	keys, err := scanKeys(context.Background(), client, instance, &state, "foo*", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"foo"}) || state.Current != 1 {
		t.Fatal(keys, state)
	}
	want := []string{"SCAN", "0", "MATCH", "foo*", "COUNT", "10", id}
	if got := <-observed; !reflect.DeepEqual(got, want) {
		t.Fatalf("SCAN args %v; want %v", got, want)
	}
	<-done
}
func TestConnectionOptions(t *testing.T) {
	o, err := baseOptions("rediss://user:secret@example.com:6380/2", 3)
	if err != nil {
		t.Fatal(err)
	}
	if o.DB != 3 || o.Username != "user" || o.Password != "secret" || o.TLSConfig == nil || o.TLSConfig.InsecureSkipVerify || o.TLSConfig.ServerName != "example.com" {
		t.Fatal("URL authentication/TLS/database options not preserved")
	}
	if _, err = openClient(context.Background(), Instance{Mode: "cluster", Addresses: []string{"redis://localhost"}}, 1); err == nil {
		t.Fatal("cluster accepted nonzero database")
	}
}
func TestTreeBinaryAndDirectoryCollision(t *testing.T) {
	entries := buildEntries([]string{"a:child", "a", "prefix:a", string([]byte{'p', ':', 255})}, "", "tree")
	if len(entries) != 4 {
		t.Fatal(entries)
	}
	if entries[0].Name != "a" || entries[0].IsDir || entries[1].Name != "a" || !entries[1].IsDir {
		t.Fatal(fmt.Sprint(entries))
	}
	entries = buildEntries([]string{string([]byte{'p', ':', 255})}, "p:", "tree")
	if len(entries) != 1 || entries[0].KeyB64 == nil {
		t.Fatal("binary leaf lost")
	}
}

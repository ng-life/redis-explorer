package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func connectionIndex(names []string, name string) (int, error) {
	if name == "" && len(names) == 1 {
		return 0, nil
	}
	found := -1
	for id, candidate := range names {
		if candidate == name {
			if found >= 0 {
				return 0, fmt.Errorf("连接名称重复，请修改配置")
			}
			found = id
		}
	}
	if found < 0 {
		return 0, fmt.Errorf("请指定 list_connections 中的 connection 名称")
	}
	return found, nil
}
func readTool(name, description string) *mcp.Tool {
	return &mcp.Tool{Name: name, Description: description, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}
}

type redisInput struct {
	Connection string  `json:"connection,omitempty" jsonschema:"配置中的连接名称；单连接可省略"`
	Database   *int    `json:"database,omitempty" jsonschema:"数据库编号，省略使用实例默认值；Cluster只支持0"`
	Prefix     string  `json:"prefix,omitempty" jsonschema:"key字面前缀"`
	View       string  `json:"view,omitempty" jsonschema:"tree（默认）或flat"`
	Cursor     string  `json:"cursor,omitempty" jsonschema:"上次结果返回的游标；扫描结束后不再传入"`
	Key        *string `json:"key,omitempty" jsonschema:"key名称，支持空字符串；与key_b64互斥"`
	KeyB64     *string `json:"key_b64,omitempty" jsonschema:"二进制key的无填充URL-safe Base64；与key互斥"`
}

func (a application) mcpHandler() http.Handler {
	server := mcp.NewServer(&mcp.Implementation{Name: "redis-explorer", Version: "dev"}, &mcp.ServerOptions{Instructions: "先 list_connections 选择连接。仅支持固定工具，不执行任意Redis命令。按游标分页，SCAN可能重复且不是快照。删除仅由配置开启，失败或超时后先核对状态，不自动重试。"})
	mcp.AddTool(server, readTool("list_connections", "列出配置实例名称、模式、默认数据库及删除开关，不返回URL或凭据、不连接Redis。"), func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		rows := []map[string]any{}
		for _, i := range a.config.Instances {
			rows = append(rows, map[string]any{"name": i.Name, "mode": i.Mode, "database": i.Database})
		}
		return nil, map[string]any{"connections": rows, "allow_delete": a.config.AllowDelete, "page_size": a.config.PageSize}, nil
	})
	tools := []*mcp.Tool{
		readTool("list_databases", "列出实例数据库和key数；Cluster只返回数据库0。"),
		readTool("list_keys", "按prefix字面前缀分页SCAN，view为tree或flat。保留返回的cursor继续扫描，cursor为空表示结束；中间页可能为空，SCAN可能重复。"),
		readTool("get_value", "分页读取key类型、TTL和值，支持string/hash/list/set/zset/stream；二进制值使用Base64。cursor语义随类型变化，使用返回值继续读取。"),
		readTool("export_key", "导出key。string使用Base64；list/zset最多10000项，stream最多1000项，hash/set完整导出，可能产生较大结果。"),
	}
	if a.config.AllowDelete {
		destructive := true
		tools = append(tools, &mcp.Tool{Name: "delete_key", Description: "使用UNLINK删除单个key；真实写操作。失败或超时后核对状态，不自动重试。", Annotations: &mcp.ToolAnnotations{DestructiveHint: &destructive}})
	}
	for _, tool := range tools {
		properties := map[string]any{
			"connection": map[string]any{"type": "string", "description": "配置中的连接名称；仅单连接可省略"},
			"database":   map[string]any{"type": "integer", "description": "默认使用实例配置；Cluster仅支持0"},
		}
		if tool.Name == "list_keys" {
			properties["prefix"] = map[string]any{"type": "string", "description": "key字面前缀，默认空字符串"}
			properties["view"] = map[string]any{"type": "string", "description": "tree（默认）或flat"}
		} else if tool.Name != "list_databases" {
			properties["key"] = map[string]any{"type": "string", "description": "key名称，可为空；与key_b64互斥"}
			properties["key_b64"] = map[string]any{"type": "string", "description": "无填充URL-safe Base64二进制key；与key互斥"}
		}
		if tool.Name == "list_keys" || tool.Name == "get_value" {
			properties["cursor"] = map[string]any{"type": "string", "description": "上页返回的游标；省略从头读取"}
		}
		tool.InputSchema = map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in redisInput) (*mcp.CallToolResult, any, error) {
			names := []string{}
			for _, i := range a.config.Instances {
				names = append(names, i.Name)
			}
			id, err := connectionIndex(names, in.Connection)
			if err != nil {
				return nil, nil, err
			}
			if in.Key != nil && in.KeyB64 != nil {
				return nil, nil, fmt.Errorf("key与key_b64互斥")
			}
			if in.View != "" && in.View != "tree" && in.View != "flat" {
				return nil, nil, fmt.Errorf("view必须为tree或flat")
			}
			operation := map[string]string{"list_databases": "databases", "list_keys": "keys", "get_value": "value", "export_key": "export", "delete_key": "delete"}[tool.Name]
			if operation != "databases" && operation != "keys" && in.Key == nil && in.KeyB64 == nil {
				return nil, nil, fmt.Errorf("必须指定key或key_b64")
			}
			input := redisOperationInput{Database: in.Database, Prefix: in.Prefix, View: in.View, Key: in.Key, KeyB64: in.KeyB64}
			if in.Cursor != "" {
				input.Cursor = &in.Cursor
			}
			out, err := a.runOperation(ctx, id, operation, input)
			if err == nil {
				if download, ok := out.(exportResult); ok {
					if download.ContentType == "application/json" {
						out = json.RawMessage(download.Body)
					} else {
						out = map[string]any{"encoding": "base64", "data": base64.StdEncoding.EncodeToString(download.Body), "content_type": download.ContentType}
					}
				}
				out = map[string]any{"result": out}
			}

			return nil, out, err
		})
	}
	return protectMCP(a.config.Listen, mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
}

// protectMCP enforces same-origin access, loopback Host validation and a 1 MiB envelope.
func protectMCP(listen string, transport http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reject browser-origin cross-site requests before the SDK creates a session.
		// Loopback-bound servers additionally reject foreign Host headers (DNS rebinding).
		host, _, _ := net.SplitHostPort(listen)
		if host == "localhost" || net.ParseIP(host).IsLoopback() {
			requestHost := r.Host
			if h, _, err := net.SplitHostPort(r.Host); err == nil {
				requestHost = h
			}
			if requestHost != "localhost" && !net.ParseIP(strings.Trim(requestHost, "[]")).IsLoopback() {
				http.Error(w, "MCP Host 不允许", 403)
				return
			}
		}
		if origins := r.Header.Values("Origin"); len(origins) > 0 {
			origin, err := url.Parse(origins[0])
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if len(origins) != 1 || err != nil || origin.Scheme != scheme || !strings.EqualFold(origin.Host, r.Host) || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
				http.Error(w, "MCP Origin 不允许", 403)
				return
			}
		}
		if r.Method == http.MethodPost {
			// Bound and read the envelope here: the compatible SDK maps body
			// read failures to 500; oversized requests should receive HTTP 413.
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024*1024))
			r.Body.Close()
			if err != nil {
				var limit *http.MaxBytesError
				if errors.As(err, &limit) {
					http.Error(w, "MCP 请求不超过 1 MiB", http.StatusRequestEntityTooLarge)
				} else {
					http.Error(w, "MCP 请求读取失败", 400)
				}
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		transport.ServeHTTP(w, r)
	})
}

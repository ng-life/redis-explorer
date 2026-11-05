# Redis Explorer

一个使用 Go 编写的 Redis Web 浏览器。支持多个 Redis 独立实例、Sentinel 和 Cluster，通过浏览器查看数据库、按 `:` 分层或平铺浏览 key，并查看常见数据类型。Cluster 模式会逐个主节点使用游标扫描。

## 配置

```sh
cp config.example.toml config.toml
```

配置支持多个实例。`addresses` 项是 Redis 连接 URL，使用 `redis://` 或 `rediss://`，可在 URL 中提供用户名、密码和数据库。Sentinel 通过 `master_name` 发现当前主节点，主节点认证信息可单独配置。Cluster 只支持 DB 0。

```toml
listen = "127.0.0.1:8080"
allow_delete = false
page_size = 100

[[instances]]
name = "开发环境"
mode = "standalone"
addresses = ["redis://:password@127.0.0.1:6379/0"]
database = 0
```

支持的 `mode`：`standalone`、`sentinel`、`cluster`。删除 key 默认关闭；需要时可在配置中设为 `allow_delete = true`，或使用 `--allow-delete`。腾讯云旧版代理如要求 `SCAN` 命令附带节点 ID，可在对应 Cluster 实例中设置 `scan_node_id = true`；默认关闭，标准 Redis Cluster 无需开启。重新开启风险操作时请确保服务只对可信网络开放。

## 启动

```sh
go run . --config config.toml
```

也可用命令行重复指定 standalone 实例：

```sh
go run . --listen 127.0.0.1:8080 --redis 'local=redis://127.0.0.1:6379/0'
```

打开 <http://127.0.0.1:8080>。

## MCP 接入

沿用原有启动命令，同一进程和端口同时提供 Web、HTTP API 和 `/mcp`。默认地址为 `http://127.0.0.1:8080/mcp`。

共用现有配置和连接，不需要独立 MCP 进程。客户端使用 Streamable HTTP 连接；不支持 stdio，也不需要为 MCP 指定子命令。先调用 `list_connections` 选择 `connection` 名称：只有一个连接时可省略，多个连接时必须指定，重名时报错。本地列表不连接数据库，也不返回凭据。

客户端配置示例（具体字段以客户端支持的配置格式为准）：

```json
{
  "mcpServers": {
    "redis-explorer": {
      "url": "http://127.0.0.1:8080/mcp"
    }
  }
}
```

Web 与 MCP 分别适配传输格式，直接调用共享业务入口（`operations.go`），共用参数校验、连接生命周期、超时和错误脱敏。成功结果在 `structuredContent` 中返回；数据工具使用 `{"result": ...}` 包装现有 API 数据，`list_connections` 返回 `connections`。参数 schema 拒绝未知字段，业务失败通过工具错误返回。

单次 MCP 请求体最多 1 MiB。MCP 校验 `Origin`，拒绝跨站请求；监听回环地址时还校验 `Host`，防止 DNS 重绑定。原生客户端可以不发送 `Origin`。服务没有内置登录；远程访问使用外部认证代理或网络访问控制，代理需转发 `/mcp` 和 MCP 协议头。默认保持回环监听，不将服务直接暴露公网。

| MCP 工具 | 参数 | 对应 HTTP API |
| --- | --- | --- |
| `list_connections` | 无 | `/api/instances` 与 `/api/settings`；返回名称、模式、默认数据库、删除开关和每页大小 |
| `list_databases` | `connection`、可选 `database` | `GET /api/instances/{id}/databases` |
| `list_keys` | `connection`、可选 `database`、`prefix`、`view`、`cursor` | `GET /api/instances/{id}/keys` |
| `get_value` | `connection`、`key` 或 `key_b64`，可选 `database`、`cursor` | `GET /api/instances/{id}/value` |
| `export_key` | `connection`、`key` 或 `key_b64`，可选 `database` | `GET /api/instances/{id}/download` |
| `delete_key` | `connection`、`key` 或 `key_b64`，可选 `database` | `DELETE /api/instances/{id}/keys`；仅开启删除时注册 |

当前程序只有 Web 服务启动参数，没有一次性读取 CLI；本次保留该契约。MCP 与 Web 的业务能力对应上表。Web 的文件下载在 MCP 中由 `export_key` 返回 JSON；原始 string 导出使用 `encoding = "base64"`、`data` 和 `content_type`，其他类型沿用 API 的 JSON 数据。

`database` 省略时使用实例配置，Cluster 仅支持数据库 0。`key` 与 `key_b64` 互斥；`key` 支持空字符串，二进制 key 使用无填充 URL-safe Base64。二进制值按现有 API 使用 Base64，不替换无效 UTF-8。

`list_keys` 默认 `view = "tree"`，也支持 `flat`；`prefix` 是字面前缀。分页使用配置的 `page_size`，`cursor` 使用上页返回值。扫描中间页可能为空，只有返回的 `cursor` 为 `null` 才表示结束；SCAN 可能重复，结果不保证快照一致。`get_value` 的游标随类型变化，应直接使用返回值。每个读取请求共用 30 秒超时，不自动遍历所有页。导出沿用现有限制：list/zset 最多 10000 项，stream 最多 1000 项，hash/set 完整导出，string 完整读取；较大的导出可能占用较多内存。

`delete_key` 使用 UNLINK 删除单个 key，是实际写操作。仅 `allow_delete = true` 或 `--allow-delete` 时注册，关闭后工具列表中不出现，底层 API 仍校验删除开关。删除失败或超时后先核对 key 状态，不自动重试。

调用 `get_value` 的参数示例：

```json
{"connection":"开发环境","database":0,"key":"order:123"}
```

## 构建与验证

需要 Go 1.27.2 或更新版本。网页通过 `go:embed` 嵌入二进制。

```sh
go vet ./...
go test -race ./...
go build -trimpath -ldflags="-s -w" -o redis-explorer .
```

Release 提供 Linux x86_64 和 macOS Apple Silicon 单文件二进制，网页静态资源编译进可执行文件。

测试会在临时目录中启动独立 Redis、Sentinel 和三节点 Cluster，只操作测试实例。需要本机安装 `redis-server` 和 `redis-cli`；缺少命令时集成测试会跳过。

兼容原有 TOML 配置、命令行参数和 HTTP API。列表和有序集合下载最多 10000 项，Stream 最多 1000 项，Hash 和 Set 完整导出。

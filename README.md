# Redis Explorer

一个使用 Rust 编写的 Redis Web 浏览器。支持多个 Redis 独立实例、Sentinel 和 Cluster，通过浏览器查看数据库、按 `:` 分层浏览 key，并查看常见数据类型。

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

支持的 `mode`：`standalone`、`sentinel`、`cluster`。删除 key 默认关闭；需要时可在配置中设为 `allow_delete = true`，或使用 `--allow-delete`。重新开启风险操作时请确保服务只对可信网络开放。

## 启动

```sh
cargo run -- --config config.toml
```

也可用命令行重复指定 standalone 实例：

```sh
cargo run -- --listen 127.0.0.1:8080 --redis 'local=redis://127.0.0.1:6379/0'
```

打开 <http://127.0.0.1:8080>。

## 构建

```sh
cargo build --locked --release
```

Release 提供 Linux x86_64 和 macOS Apple Silicon 单文件二进制，网页静态资源编译进可执行文件。

use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{Html, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use clap::Parser;
use redis::{
    aio::MultiplexedConnection,
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    cluster_routing::{RoutingInfo, SingleNodeRoutingInfo},
    sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType},
    Client as RedisClient, Cmd, RedisConnectionInfo, TlsMode, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "redis-explorer", about = "A web UI for browsing Redis keys")]
struct Args {
    /// TOML configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    /// Web server bind address (overrides config)
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Standalone Redis instance, repeatable (overrides configured instances)
    #[arg(long = "redis", value_name = "NAME=URL")]
    instances: Vec<String>,
    /// Enable deletion of individual keys
    #[arg(long, conflicts_with = "disable_delete")]
    allow_delete: bool,
    /// Disable deletion of individual keys
    #[arg(long, conflicts_with = "allow_delete")]
    disable_delete: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum RedisMode {
    Standalone,
    Sentinel,
    Cluster,
}

impl Default for RedisMode {
    fn default() -> Self {
        Self::Standalone
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Instance {
    name: String,
    #[serde(default)]
    mode: RedisMode,
    #[serde(default)]
    addresses: Vec<String>,
    #[serde(default)]
    database: i64,
    master_name: Option<String>,
    username: Option<String>,
    password: Option<String>,
    #[serde(default)]
    tls: bool,
    /// Append the Redis Cluster node ID to SCAN (for compatible proxy extensions).
    #[serde(default)]
    scan_node_id: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    #[serde(default)]
    instances: Vec<Instance>,
    #[serde(default)]
    allow_delete: bool,
    #[serde(default = "default_page_size")]
    page_size: usize,
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:8080".parse().expect("valid default address")
}

fn default_page_size() -> usize {
    100
}

#[derive(Clone)]
struct AppState {
    config: Arc<RwLock<Config>>,
}

#[derive(Serialize)]
struct InstanceSummary {
    id: usize,
    name: String,
    mode: RedisMode,
}

enum RedisConnection {
    Direct(MultiplexedConnection),
    Cluster(ClusterConnection),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScanNode {
    host: String,
    port: u16,
    node_id: Option<String>,
    cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScanState {
    nodes: Vec<ScanNode>,
    current: usize,
    #[serde(default)]
    use_node_id: bool,
}

#[derive(Serialize)]
struct KeyEntry {
    name: String,
    key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_b64: Option<String>,
    is_dir: bool,
}

#[derive(Serialize)]
struct KeyPage {
    prefix: String,
    view: String,
    nodes: Vec<KeyEntry>,
    cursor: Option<String>,
    scanned: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let mut config = if args.config.exists() {
        toml::from_str::<Config>(&std::fs::read_to_string(&args.config)?)?
    } else {
        Config {
            listen: default_listen(),
            instances: Vec::new(),
            allow_delete: false,
            page_size: default_page_size(),
        }
    };
    if let Some(listen) = args.listen {
        config.listen = listen;
    }
    if !args.instances.is_empty() {
        config.instances = args
            .instances
            .into_iter()
            .map(|value| {
                let (name, address) = value.split_once('=').unwrap_or((&value, &value));
                Instance {
                    name: name.to_string(),
                    mode: RedisMode::Standalone,
                    addresses: vec![address.to_string()],
                    database: 0,
                    master_name: None,
                    username: None,
                    password: None,
                    tls: address.starts_with("rediss://"),
                    scan_node_id: false,
                }
            })
            .collect();
    }
    if args.allow_delete {
        config.allow_delete = true;
    } else if args.disable_delete {
        config.allow_delete = false;
    }
    config.page_size = config.page_size.clamp(10, 500);
    validate_config(&config)?;

    let state = AppState {
        config: Arc::new(RwLock::new(config)),
    };
    let listen = state.config.read().await.listen;
    let app = Router::new()
        .route("/", get(index))
        .route("/api/instances", get(list_instances))
        .route("/api/settings", get(get_settings))
        .route("/api/instances/{id}/databases", get(list_databases))
        .route(
            "/api/instances/{id}/keys",
            get(list_keys).delete(delete_key),
        )
        .route("/api/instances/{id}/value", get(get_value))
        .route("/api/instances/{id}/download", get(download_value))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(address = %listen, "Redis Explorer is ready");
    axum::serve(listener, app).await?;
    Ok(())
}

fn validate_config(config: &Config) -> Result<(), String> {
    for (index, instance) in config.instances.iter().enumerate() {
        if instance.name.trim().is_empty() || instance.addresses.is_empty() {
            return Err(format!(
                "instances[{index}] requires a name and at least one address"
            ));
        }
        if matches!(instance.mode, RedisMode::Sentinel)
            && instance
                .master_name
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            return Err(format!(
                "instances[{index}] requires master_name in sentinel mode"
            ));
        }
        if instance.database < 0 {
            return Err(format!("instances[{index}].database must be non-negative"));
        }
        if matches!(instance.mode, RedisMode::Cluster) && instance.database != 0 {
            return Err(format!(
                "instances[{index}]: Redis Cluster only supports database 0"
            ));
        }
    }
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn list_instances(State(state): State<AppState>) -> Json<Vec<InstanceSummary>> {
    let config = state.config.read().await;
    Json(
        config
            .instances
            .iter()
            .enumerate()
            .map(|(id, instance)| InstanceSummary {
                id,
                name: instance.name.clone(),
                mode: instance.mode.clone(),
            })
            .collect(),
    )
}

#[derive(Serialize)]
struct Settings {
    allow_delete: bool,
    page_size: usize,
}

async fn get_settings(State(state): State<AppState>) -> Json<Settings> {
    let config = state.config.read().await;
    Json(Settings {
        allow_delete: config.allow_delete,
        page_size: config.page_size,
    })
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

fn api_error(status: StatusCode, message: impl Into<String>) -> (StatusCode, String) {
    (status, message.into())
}

fn redis_error(error: redis::RedisError) -> (StatusCode, String) {
    tracing::warn!(kind = ?error.kind(), error = %error, "Redis request failed");
    api_error(
        StatusCode::BAD_GATEWAY,
        "Redis request failed; check address, credentials, TLS, and ACL permissions",
    )
}

fn parse_error(_error: redis::ParsingError) -> (StatusCode, String) {
    api_error(StatusCode::BAD_GATEWAY, "Unexpected response from Redis")
}

async fn find_instance(state: &AppState, id: usize) -> ApiResult<(Instance, usize)> {
    let config = state.config.read().await;
    let instance = config
        .instances
        .get(id)
        .cloned()
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Unknown Redis instance"))?;
    Ok((instance, config.page_size))
}

async fn open_connection(
    instance: &Instance,
    database: i64,
) -> redis::RedisResult<RedisConnection> {
    match instance.mode {
        RedisMode::Standalone => {
            let client = RedisClient::open(instance.addresses[0].as_str())?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            if database != 0 {
                redis::cmd("SELECT")
                    .arg(database)
                    .query_async::<()>(&mut connection)
                    .await?;
            }
            Ok(RedisConnection::Direct(connection))
        }
        RedisMode::Sentinel => {
            let mut node_info = RedisConnectionInfo::default().set_db(database);
            if let Some(username) = &instance.username {
                node_info = node_info.set_username(username);
            }
            if let Some(password) = &instance.password {
                node_info = node_info.set_password(password);
            }
            let mut target_info =
                SentinelNodeConnectionInfo::default().set_redis_connection_info(node_info);
            if instance.tls {
                target_info = target_info.set_tls_mode(TlsMode::Secure);
            }
            let mut client = SentinelClient::build(
                instance.addresses.clone(),
                instance.master_name.clone().unwrap_or_default(),
                Some(target_info),
                SentinelServerType::Master,
            )?;
            let redis_client = client.async_get_client().await?;
            Ok(RedisConnection::Direct(
                redis_client.get_multiplexed_async_connection().await?,
            ))
        }
        RedisMode::Cluster => {
            let client = ClusterClient::new(instance.addresses.clone())?;
            Ok(RedisConnection::Cluster(
                client.get_async_connection().await?,
            ))
        }
    }
}

async fn execute(connection: &mut RedisConnection, command: Cmd) -> redis::RedisResult<Value> {
    match connection {
        RedisConnection::Direct(connection) => command.query_async(connection).await,
        RedisConnection::Cluster(connection) => command.query_async(connection).await,
    }
}

async fn select_database(
    connection: &mut RedisConnection,
    instance: &Instance,
    database: i64,
) -> redis::RedisResult<()> {
    if matches!(instance.mode, RedisMode::Cluster) && database != 0 {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis Cluster only supports database 0",
        )));
    }
    if !matches!(instance.mode, RedisMode::Cluster) {
        let mut command = redis::cmd("SELECT");
        command.arg(database);
        execute(connection, command).await?;
    }
    Ok(())
}

#[derive(Serialize)]
struct DatabaseEntry {
    id: i64,
    keys: Option<u64>,
}

async fn list_databases(
    State(state): State<AppState>,
    Path(id): Path<usize>,
) -> ApiResult<Json<Vec<DatabaseEntry>>> {
    let (instance, _) = find_instance(&state, id).await?;
    let mut connection = open_connection(&instance, instance.database)
        .await
        .map_err(redis_error)?;
    if matches!(instance.mode, RedisMode::Cluster) {
        return Ok(Json(vec![DatabaseEntry { id: 0, keys: None }]));
    }
    let mut command = redis::cmd("INFO");
    command.arg("keyspace");
    let response: String = redis::from_redis_value(
        execute(&mut connection, command)
            .await
            .map_err(redis_error)?,
    )
    .map_err(parse_error)?;
    let mut databases = BTreeMap::new();
    for line in response.lines().filter(|line| line.starts_with("db")) {
        if let Some((db, fields)) = line.split_once(':') {
            let id = db.trim_start_matches("db").parse::<i64>().ok();
            let keys = fields.split(',').find_map(|field| {
                field
                    .strip_prefix("keys=")
                    .and_then(|value| value.parse().ok())
            });
            if let Some(id) = id {
                databases.insert(id, keys);
            }
        }
    }
    databases.entry(instance.database).or_insert(None);
    Ok(Json(
        databases
            .into_iter()
            .map(|(id, keys)| DatabaseEntry { id, keys })
            .collect(),
    ))
}

fn parse_scan_state(encoded: Option<&String>) -> ApiResult<Option<ScanState>> {
    let Some(encoded) = encoded else {
        return Ok(None);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "Invalid scan cursor"))?;
    let state = serde_json::from_slice(&bytes)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "Invalid scan cursor"))?;
    Ok(Some(state))
}

fn encode_scan_state(state: &ScanState) -> Option<String> {
    if state.current >= state.nodes.len() {
        return None;
    }
    serde_json::to_vec(state)
        .ok()
        .map(|json| URL_SAFE_NO_PAD.encode(json))
}

fn glob_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn value_bytes(value: &Value) -> Option<&[u8]> {
    match value {
        Value::BulkString(bytes) => Some(bytes),
        Value::SimpleString(text) => Some(text.as_bytes()),
        _ => None,
    }
}

fn value_text(value: &Value) -> Option<String> {
    value_bytes(value).and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
}

fn value_int(value: &Value) -> Option<i64> {
    match value {
        Value::Int(number) => Some(*number),
        Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        Value::SimpleString(text) => text.parse().ok(),
        _ => None,
    }
}

async fn scan_keys(
    connection: &mut RedisConnection,
    state: &mut ScanState,
    pattern: &str,
    batch_size: usize,
) -> redis::RedisResult<(Vec<Vec<u8>>, usize)> {
    let mut keys = Vec::new();
    let mut scanned = 0;
    let mut rounds = 0;
    while keys.len() < batch_size && state.current < state.nodes.len() && rounds < 8 {
        rounds += 1;
        let node_index = state.current;
        let node = &state.nodes[node_index];
        let mut command = redis::cmd("SCAN");
        command
            .arg(node.cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(batch_size);
        if state.use_node_id {
            if let Some(node_id) = &node.node_id {
                command.arg(node_id);
            }
        }
        let response = match connection {
            RedisConnection::Cluster(cluster) => {
                let routing = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                    host: node.host.clone(),
                    port: node.port,
                });
                cluster.route_command(command, routing).await?
            }
            RedisConnection::Direct(direct) => command.query_async(direct).await?,
        };
        // Redis keys are arbitrary bytes. Keep the raw bytes so a binary key
        // cannot make conversion of the whole SCAN page fail.
        let (next_cursor, page): (u64, Vec<Vec<u8>>) = redis::from_redis_value(response)?;
        let node = &mut state.nodes[node_index];
        node.cursor = next_cursor;
        scanned += page.len();
        keys.extend(page);
        if next_cursor == 0 {
            state.current += 1;
        }
    }
    Ok((keys, scanned))
}

async fn discover_cluster_nodes(
    connection: &mut RedisConnection,
) -> redis::RedisResult<Vec<ScanNode>> {
    let mut command = redis::cmd("CLUSTER");
    command.arg("NODES");
    let response = execute(connection, command).await?;
    let mut nodes = BTreeMap::new();
    let cluster_nodes = value_text(&response).ok_or_else(|| {
        redis::RedisError::from((
            redis::ErrorKind::Client,
            "CLUSTER NODES returned an unexpected response",
        ))
    })?;
    for line in cluster_nodes.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 3 {
            continue;
        }
        let flags = fields[2].split(',').collect::<Vec<_>>();
        if !flags.contains(&"master")
            || flags
                .iter()
                .any(|flag| matches!(*flag, "fail" | "fail?" | "handshake" | "noaddr"))
        {
            continue;
        }
        let address = fields[1]
            .split('@')
            .next()
            .unwrap_or(fields[1])
            .split(',')
            .next()
            .unwrap_or(fields[1]);
        let Some((host, port)) = address.rsplit_once(':') else {
            continue;
        };
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let Ok(port) = port.parse::<u16>() else {
            continue;
        };
        nodes
            .entry(fields[0].to_string())
            .or_insert_with(|| ScanNode {
                host,
                port,
                node_id: Some(fields[0].to_string()),
                cursor: 0,
            });
    }
    if nodes.is_empty() {
        return Err(redis::RedisError::from((
            redis::ErrorKind::Client,
            "No primary nodes were returned by CLUSTER NODES",
        )));
    }
    Ok(nodes.into_values().collect())
}

async fn discover_cluster_primaries(
    connection: &mut RedisConnection,
) -> redis::RedisResult<Vec<ScanNode>> {
    let mut command = redis::cmd("CLUSTER");
    command.arg("SLOTS");
    let response = execute(connection, command).await?;
    let slots: Vec<Vec<Value>> = redis::from_redis_value(response)?;
    let mut primaries = BTreeMap::new();
    for slot in slots {
        let Some(Value::Array(primary)) = slot.get(2) else {
            continue;
        };
        let Some(host) = primary.first().and_then(value_text) else {
            continue;
        };
        let Some(port) = primary
            .get(1)
            .and_then(value_int)
            .and_then(|port| u16::try_from(port).ok())
        else {
            continue;
        };
        primaries.entry((host.clone(), port)).or_insert(ScanNode {
            host,
            port,
            node_id: None,
            cursor: 0,
        });
    }
    if primaries.is_empty() {
        return Err(redis::RedisError::from((
            redis::ErrorKind::Client,
            "No primary nodes were returned by CLUSTER SLOTS",
        )));
    }
    Ok(primaries.into_values().collect())
}

async fn list_keys(
    State(state): State<AppState>,
    Path(id): Path<usize>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<KeyPage>> {
    let (instance, page_size) = find_instance(&state, id).await?;
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let view = if query.get("view").is_some_and(|value| value == "flat") {
        "flat"
    } else {
        "tree"
    };
    if prefix.contains('\0') {
        return Err(api_error(StatusCode::BAD_REQUEST, "Invalid key prefix"));
    }
    let database = query
        .get("db")
        .and_then(|value| value.parse().ok())
        .unwrap_or(instance.database);
    let mut connection = open_connection(&instance, database)
        .await
        .map_err(redis_error)?;
    select_database(&mut connection, &instance, database)
        .await
        .map_err(redis_error)?;
    let mut scan_state = parse_scan_state(query.get("cursor"))?.unwrap_or_else(|| ScanState {
        nodes: vec![ScanNode {
            host: String::new(),
            port: 0,
            node_id: None,
            cursor: 0,
        }],
        current: 0,
        use_node_id: instance.scan_node_id,
    });
    if matches!(instance.mode, RedisMode::Cluster) && query.get("cursor").is_none() {
        scan_state.use_node_id = instance.scan_node_id;
        scan_state.nodes = if instance.scan_node_id {
            discover_cluster_nodes(&mut connection).await
        } else {
            discover_cluster_primaries(&mut connection).await
        }
        .map_err(redis_error)?;
    }
    let pattern = format!("{}*", glob_escape(&prefix));
    let (keys, scanned) = scan_keys(&mut connection, &mut scan_state, &pattern, page_size)
        .await
        .map_err(redis_error)?;
    let mut children: BTreeMap<(String, bool), (Option<String>, Option<String>)> = BTreeMap::new();
    for key in keys {
        let Some(remainder) = key.strip_prefix(prefix.as_bytes()) else {
            continue;
        };
        if remainder.is_empty() {
            continue;
        }
        if view == "flat" {
            let key_text = String::from_utf8(key.clone()).ok();
            let key_b64 = key_text.is_none().then(|| URL_SAFE_NO_PAD.encode(&key));
            let display = key_text.clone().unwrap_or_else(|| {
                format!("[binary key: {}]", key_b64.as_deref().unwrap_or_default())
            });
            children.insert((display, false), (key_text, key_b64));
        } else if let Some(separator) = remainder.iter().position(|byte| *byte == b':') {
            let name = &remainder[..separator];
            if !name.is_empty() {
                if let Ok(name) = String::from_utf8(name.to_vec()) {
                    children.entry((name, true)).or_insert((None, None));
                } else {
                    let encoded = URL_SAFE_NO_PAD.encode(&key);
                    children.insert(
                        (format!("[binary key: {encoded}]"), false),
                        (None, Some(encoded)),
                    );
                }
            }
        } else {
            match String::from_utf8(key.clone()) {
                Ok(key_text) => {
                    children.insert(
                        (String::from_utf8_lossy(remainder).into_owned(), false),
                        (Some(key_text), None),
                    );
                }
                Err(_) => {
                    let encoded = URL_SAFE_NO_PAD.encode(&key);
                    children.insert(
                        (format!("[binary key: {encoded}]"), false),
                        (None, Some(encoded)),
                    );
                }
            }
        }
    }
    let nodes = children
        .into_iter()
        .map(|((name, is_dir), (key, key_b64))| KeyEntry {
            name,
            key,
            key_b64,
            is_dir,
        })
        .collect();
    Ok(Json(KeyPage {
        prefix,
        view: view.to_string(),
        nodes,
        cursor: encode_scan_state(&scan_state),
        scanned,
    }))
}

fn redis_value_json(value: &Value) -> JsonValue {
    match value {
        Value::Nil => JsonValue::Null,
        Value::Int(number) => json!(number),
        Value::Double(number) => json!(number),
        Value::Boolean(value) => json!(value),
        Value::Okay => json!("OK"),
        Value::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => json!({"text": text, "encoding": "utf-8"}),
            Err(_) => {
                json!({"base64": base64::engine::general_purpose::STANDARD.encode(bytes), "encoding": "base64"})
            }
        },
        Value::SimpleString(text) => json!({"text": text, "encoding": "utf-8"}),
        Value::Array(values) | Value::Set(values) => {
            JsonValue::Array(values.iter().map(redis_value_json).collect())
        }
        Value::Map(values) => {
            let mut map = Map::new();
            for (key, value) in values {
                map.insert(
                    value_text(key).unwrap_or_else(|| format!("key_{}", map.len())),
                    redis_value_json(value),
                );
            }
            JsonValue::Object(map)
        }
        Value::Attribute { data, .. } => redis_value_json(data),
        Value::VerbatimString { text, .. } => json!({"text": text, "encoding": "utf-8"}),
        Value::BigNumber(number) => json!(String::from_utf8_lossy(number)),
        Value::Push { data, .. } => JsonValue::Array(data.iter().map(redis_value_json).collect()),
        Value::ServerError(error) => json!({"error": error.to_string()}),
        _ => JsonValue::Null,
    }
}

#[derive(Serialize)]
struct ValuePage {
    key: String,
    kind: String,
    ttl_ms: i64,
    total: Option<i64>,
    cursor: Option<String>,
    items: JsonValue,
}

fn query_key(query: &BTreeMap<String, String>) -> ApiResult<(Vec<u8>, String)> {
    if let Some(encoded) = query.get("key_b64") {
        let key = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| api_error(StatusCode::BAD_REQUEST, "Invalid encoded key"))?;
        return Ok((key, format!("base64:{encoded}")));
    }
    let key = query
        .get("key")
        .cloned()
        .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "Missing key"))?;
    Ok((key.as_bytes().to_vec(), key))
}

async fn get_value(
    State(state): State<AppState>,
    Path(id): Path<usize>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<ValuePage>> {
    let (instance, page_size) = find_instance(&state, id).await?;
    let (key, display_key) = query_key(&query)?;
    let database = query
        .get("db")
        .and_then(|value| value.parse().ok())
        .unwrap_or(instance.database);
    let raw_cursor = query.get("cursor").cloned();
    let offset = raw_cursor
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    let mut connection = open_connection(&instance, database)
        .await
        .map_err(redis_error)?;
    select_database(&mut connection, &instance, database)
        .await
        .map_err(redis_error)?;

    let mut type_command = redis::cmd("TYPE");
    type_command.arg(key.as_slice());
    let kind = value_text(
        &execute(&mut connection, type_command)
            .await
            .map_err(redis_error)?,
    )
    .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Key no longer exists"))?;
    let mut ttl_command = redis::cmd("PTTL");
    ttl_command.arg(key.as_slice());
    let ttl_ms = value_int(
        &execute(&mut connection, ttl_command)
            .await
            .map_err(redis_error)?,
    )
    .unwrap_or(-1);
    let mut cursor = None;
    let (items, total) = match kind.as_str() {
        "string" => {
            let mut len_command = redis::cmd("STRLEN");
            len_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, len_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut get_command = redis::cmd("GETRANGE");
            get_command
                .arg(key.as_slice())
                .arg(offset)
                .arg(offset + page_size as i64 - 1);
            let data = execute(&mut connection, get_command)
                .await
                .map_err(redis_error)?;
            let json = redis_value_json(&data);
            let next = offset + page_size as i64;
            cursor = (next < total).then(|| next.to_string());
            (json, Some(total))
        }
        "hash" => {
            let mut len_command = redis::cmd("HLEN");
            len_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, len_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut scan = redis::cmd("HSCAN");
            scan.arg(key.as_slice())
                .arg(offset)
                .arg("COUNT")
                .arg(page_size);
            let (next, pairs): (u64, Vec<Value>) =
                redis::from_redis_value(execute(&mut connection, scan).await.map_err(redis_error)?)
                    .map_err(parse_error)?;
            let items = pairs.chunks(2).filter_map(|pair| Some(json!({"field": redis_value_json(pair.first()?), "value": redis_value_json(pair.get(1)?)}))).collect::<Vec<_>>();
            cursor = (next != 0).then(|| next.to_string());
            (json!(items), Some(total))
        }
        "list" => {
            let mut len_command = redis::cmd("LLEN");
            len_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, len_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut range = redis::cmd("LRANGE");
            range
                .arg(key.as_slice())
                .arg(offset)
                .arg(offset + page_size as i64 - 1);
            let items: Vec<Value> = redis::from_redis_value(
                execute(&mut connection, range).await.map_err(redis_error)?,
            )
            .map_err(parse_error)?;
            let next = offset + page_size as i64;
            cursor = (next < total).then(|| next.to_string());
            (
                json!(items.iter().map(redis_value_json).collect::<Vec<_>>()),
                Some(total),
            )
        }
        "set" => {
            let mut size_command = redis::cmd("SCARD");
            size_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, size_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut scan = redis::cmd("SSCAN");
            scan.arg(key.as_slice())
                .arg(offset)
                .arg("COUNT")
                .arg(page_size);
            let (next, values): (u64, Vec<Value>) =
                redis::from_redis_value(execute(&mut connection, scan).await.map_err(redis_error)?)
                    .map_err(parse_error)?;
            cursor = (next != 0).then(|| next.to_string());
            (
                json!(values.iter().map(redis_value_json).collect::<Vec<_>>()),
                Some(total),
            )
        }
        "zset" => {
            let mut size_command = redis::cmd("ZCARD");
            size_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, size_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut range = redis::cmd("ZRANGE");
            range
                .arg(key.as_slice())
                .arg(offset)
                .arg(offset + page_size as i64 - 1)
                .arg("WITHSCORES");
            let values: Vec<Value> = redis::from_redis_value(
                execute(&mut connection, range).await.map_err(redis_error)?,
            )
            .map_err(parse_error)?;
            let items = values.chunks(2).filter_map(|pair| Some(json!({"member": redis_value_json(pair.first()?), "score": redis_value_json(pair.get(1)?)}))).collect::<Vec<_>>();
            let next = offset + page_size as i64;
            cursor = (next < total).then(|| next.to_string());
            (json!(items), Some(total))
        }
        "stream" => {
            let mut len_command = redis::cmd("XLEN");
            len_command.arg(key.as_slice());
            let total = value_int(
                &execute(&mut connection, len_command)
                    .await
                    .map_err(redis_error)?,
            )
            .unwrap_or(0);
            let mut range = redis::cmd("XRANGE");
            range
                .arg(key.as_slice())
                .arg(if raw_cursor.is_none() {
                    "-".to_string()
                } else {
                    format!("({}", raw_cursor.as_deref().unwrap_or("0-0"))
                })
                .arg("+")
                .arg("COUNT")
                .arg(page_size);
            let values: Vec<Value> = redis::from_redis_value(
                execute(&mut connection, range).await.map_err(redis_error)?,
            )
            .map_err(parse_error)?;
            if values.len() == page_size {
                cursor = values.last().and_then(|value| match value {
                    Value::Array(entry) => entry.first().and_then(value_text),
                    _ => None,
                });
            }
            (
                json!(values.iter().map(redis_value_json).collect::<Vec<_>>()),
                Some(total),
            )
        }
        _ => {
            return Err(api_error(
                StatusCode::BAD_GATEWAY,
                format!("Unsupported Redis key type: {kind}"),
            ))
        }
    };
    Ok(Json(ValuePage {
        key: display_key,
        kind,
        ttl_ms,
        total,
        cursor,
        items,
    }))
}

async fn download_value(
    State(state): State<AppState>,
    Path(id): Path<usize>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Response> {
    let (key, display_key) = query_key(&query)?;
    let (instance, _) = find_instance(&state, id).await?;
    let database = query
        .get("db")
        .and_then(|value| value.parse().ok())
        .unwrap_or(instance.database);
    let mut connection = open_connection(&instance, database)
        .await
        .map_err(redis_error)?;
    select_database(&mut connection, &instance, database)
        .await
        .map_err(redis_error)?;
    let mut type_command = redis::cmd("TYPE");
    type_command.arg(key.as_slice());
    let kind = value_text(
        &execute(&mut connection, type_command)
            .await
            .map_err(redis_error)?,
    )
    .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Key no longer exists"))?;
    let (body, content_type, extension) = if kind == "string" {
        let mut command = redis::cmd("GET");
        command.arg(key.as_slice());
        let value = execute(&mut connection, command)
            .await
            .map_err(redis_error)?;
        let bytes = value_bytes(&value).unwrap_or_default().to_vec();
        (bytes, "application/octet-stream", "bin")
    } else {
        let (value, _, _) = fetch_download_snapshot(&mut connection, &key, &display_key, &kind)
            .await
            .map_err(redis_error)?;
        (
            serde_json::to_vec_pretty(&value).unwrap_or_default(),
            "application/json",
            "json",
        )
    };
    let safe_name = format!("redis-key.{extension}");
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{safe_name}\""))
            .expect("static filename is valid"),
    );
    Ok(response)
}

async fn fetch_download_snapshot(
    connection: &mut RedisConnection,
    key: &[u8],
    display_key: &str,
    kind: &str,
) -> redis::RedisResult<(JsonValue, Option<i64>, Option<String>)> {
    let value = match kind {
        "hash" => {
            let mut cmd = redis::cmd("HGETALL");
            cmd.arg(key);
            redis_value_json(&execute(connection, cmd).await?)
        }
        "list" => {
            let mut cmd = redis::cmd("LRANGE");
            cmd.arg(key).arg(0).arg(9999);
            redis_value_json(&execute(connection, cmd).await?)
        }
        "set" => {
            let mut cmd = redis::cmd("SMEMBERS");
            cmd.arg(key);
            redis_value_json(&execute(connection, cmd).await?)
        }
        "zset" => {
            let mut cmd = redis::cmd("ZRANGE");
            cmd.arg(key).arg(0).arg(9999).arg("WITHSCORES");
            redis_value_json(&execute(connection, cmd).await?)
        }
        "stream" => {
            let mut cmd = redis::cmd("XRANGE");
            cmd.arg(key).arg("-").arg("+").arg("COUNT").arg(1000);
            redis_value_json(&execute(connection, cmd).await?)
        }
        _ => JsonValue::Null,
    };
    Ok((
        json!({"key": display_key, "type": kind, "items": value, "note": "Collections are limited to 10000 entries; streams to 1000 entries per download."}),
        None,
        None,
    ))
}

#[derive(Serialize)]
struct DeleteResponse {
    deleted: bool,
}

async fn delete_key(
    State(state): State<AppState>,
    Path(id): Path<usize>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<DeleteResponse>> {
    let (key, _) = query_key(&query)?;
    let (instance, _) = find_instance(&state, id).await?;
    if !state.config.read().await.allow_delete {
        return Err(api_error(StatusCode::FORBIDDEN, "Key deletion is disabled"));
    }
    let database = query
        .get("db")
        .and_then(|value| value.parse().ok())
        .unwrap_or(instance.database);
    let mut connection = open_connection(&instance, database)
        .await
        .map_err(redis_error)?;
    select_database(&mut connection, &instance, database)
        .await
        .map_err(redis_error)?;
    let mut command = redis::cmd("UNLINK");
    command.arg(key.as_slice());
    let removed: i64 = redis::from_redis_value(
        execute(&mut connection, command)
            .await
            .map_err(redis_error)?,
    )
    .map_err(parse_error)?;
    Ok(Json(DeleteResponse {
        deleted: removed > 0,
    }))
}

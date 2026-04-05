use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};

use near_crypto::{InMemorySigner, Signer};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::transaction::{Transaction, TransactionV0};
use near_primitives::types::BlockReference;
use near_primitives::views::QueryRequest;

// Executor imports used in execute_wasm()
use offchainvm_worker::api_client::{ExecutionOutput, ResourceLimits, ResponseFormat};
use offchainvm_worker::config::RpcProxyConfig;
use offchainvm_worker::executor::{ExecutionContext, Executor};
use offchainvm_worker::outlayer_rpc::RpcProxy;

// Dashboard imports
use axum::{
    extract::State,
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

// ── Execution Record (shared state) ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct ExecutionRecord {
    request_id: u64,
    input: String,
    output: String,
    execution_time_ms: u64,
    instructions: u64,
    timestamp: String,
    success: bool,
}

#[derive(Debug, Clone, Serialize)]
struct DaemonStatus {
    running: bool,
    uptime_secs: u64,
    poll_count: u64,
    last_poll_time: Option<String>,
    contract_id: String,
    account_id: String,
    rpc_url: String,
    poll_interval_secs: u64,
    dashboard_addr: Option<String>,
}

struct DashboardState {
    history: Mutex<Vec<ExecutionRecord>>,
    status: Mutex<DashboardStatusInner>,
    events_tx: broadcast::Sender<String>,
    storage_dir: PathBuf,
    contract_id: String,
    rpc_url: String,
}

#[derive(Debug)]
struct DashboardStatusInner {
    start_time: std::time::Instant,
    poll_count: u64,
    last_poll_time: Option<String>,
    contract_id: String,
    account_id: String,
    rpc_url: String,
    poll_interval_secs: u64,
    dashboard_addr: Option<String>,
}

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(default)]
struct Config {
    contract_id: String,
    account_id: String,
    network: String,
    rpc_url: String,
    poll_interval_secs: u64,
    wasm_search_dirs: Vec<String>,
    key_path: String,
    log_file: Option<String>,
    pid_file: Option<String>,
    /// Environment variables to pass to WASM execution
    env: HashMap<String, String>,
    /// Dashboard HTTP server bind address (e.g. "127.0.0.1:8082")
    dashboard_addr: Option<String>,
    /// Poll mode: "poll" (default) or "websocket"
    poll_mode: String,
}

impl Default for Config {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        Self {
            contract_id: "outlayer.kampouse.testnet".into(),
            account_id: "kampouse.testnet".into(),
            network: "testnet".into(),
            rpc_url: "https://rpc.testnet.near.org".into(),
            poll_interval_secs: 5,
            wasm_search_dirs: vec![format!("{}/.openclaw/workspace", home.display())],
            key_path: format!("{}/.near-credentials/testnet/kampouse.testnet.json", home.display()),
            log_file: None,
            pid_file: None,
            env: HashMap::new(),
            dashboard_addr: None,
            poll_mode: "poll".into(),
        }
    }
}

impl Config {
    fn load() -> Self {
        for dir in &[".", &dirs::home_dir().unwrap_or_default().join(".inlayer").display().to_string()] {
            for name in &["layerd.config", "layerd.config.toml"] {
                let path = PathBuf::from(dir).join(name);
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(cfg) = toml::from_str(&s) { return cfg; }
                }
            }
        }
        Config::default()
    }

    fn pid_file_path(&self) -> PathBuf {
        self.pid_file.as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = dirs::home_dir().unwrap_or_default();
                home.join(".inlayer").join("layerd.pid")
            })
    }

    fn log_file_path(&self) -> PathBuf {
        self.log_file.as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = dirs::home_dir().unwrap_or_default();
                home.join(".inlayer").join("layerd.log")
            })
    }
}

// ── Daemon ──────────────────────────────────────────────────────────────────

fn daemonize(_log_path: &Path, pid_path: &Path) -> Result<()> {
    if let Some(parent) = pid_path.parent() {
        fs::create_dir_all(parent)?;
    }

    match unsafe { libc::fork() } {
        -1 => anyhow::bail!("fork failed"),
        0 => {
            unsafe { libc::setsid() };
            unsafe { libc::close(0); libc::close(1); libc::close(2); };
            // Redirect fd 0,1,2 to /dev/null
            let dn = fs::File::open("/dev/null").ok();
            let dn_fd = dn.as_ref().map(|f| f.as_raw_fd()).unwrap_or(-1);
            unsafe {
                libc::dup(dn_fd); // stdin
                libc::dup(dn_fd); // stdout
                libc::dup(dn_fd); // stderr
            }
            fs::write(pid_path, std::process::id().to_string()).ok();
            Ok(())
        }
        _ => {
            std::thread::sleep(Duration::from_millis(200));
            std::process::exit(0);
        }
    }
}

use std::os::unix::io::AsRawFd;

// ── Key loading ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KeyFile {
    private_key: String,
    account_id: String,
}

fn load_signer(path: &str) -> Result<InMemorySigner> {
    let data = std::fs::read_to_string(path).context("reading key file")?;
    let kf: KeyFile = serde_json::from_str(&data).context("parsing key file")?;
    let signer_account_id: near_primitives::types::AccountId = kf.account_id.parse()?;
    let signer_secret_key: near_crypto::SecretKey = kf.private_key.parse()?;
    Ok(InMemorySigner::from_secret_key(signer_account_id, signer_secret_key))
}

// ── NEAR RPC ────────────────────────────────────────────────────────────────

struct Rpc {
    client: JsonRpcClient,
    rt: tokio::runtime::Runtime,
}

impl Rpc {
    fn new(url: &str) -> Result<Self> {
        Ok(Self {
            client: JsonRpcClient::connect(url),
            rt: tokio::runtime::Runtime::new()?,
        })
    }

    fn view(&self, contract: &str, method: &str, args: &[u8]) -> Result<Vec<u8>> {
        let client = &self.client;
        self.rt.block_on(async {
            let request = methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::CallFunction {
                    account_id: contract.parse()?,
                    method_name: method.to_string(),
                    args: serde_json::from_str(&format!("\"{}\"", base64::engine::general_purpose::STANDARD.encode(args)))?,
                },
            };
            let response = client.call(request).await?;
            if let QueryResponseKind::CallResult(result) = response.kind {
                Ok(result.result)
            } else {
                anyhow::bail!("unexpected query response")
            }
        })
    }

    fn send_tx(&self, signer: &InMemorySigner, contract: &str, method: &str, args: serde_json::Value, gas: u64, deposit: u128) -> Result<String> {
        let client = &self.client;
        let signer_account_id = signer.account_id.clone();
        let signer_public_key = signer.public_key.clone();
        let signer_clone = signer.clone();

        self.rt.block_on(async {
            let access_key_query = methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKey {
                    account_id: signer_account_id.clone(),
                    public_key: signer_public_key.clone(),
                },
            };
            let access_key_response = client.call(access_key_query).await?;
            let (current_nonce, block_hash) = match access_key_response.kind {
                QueryResponseKind::AccessKey(ak) => (ak.nonce, access_key_response.block_hash),
                _ => anyhow::bail!("unexpected access key response"),
            };

            let args_bytes = serde_json::to_vec(&args)?;

            let transaction = TransactionV0 {
                signer_id: signer_account_id,
                public_key: signer_public_key,
                nonce: current_nonce + 1,
                receiver_id: contract.parse()?,
                block_hash,
                actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: method.to_string(),
                    args: args_bytes,
                    gas,
                    deposit,
                }))],
            };

            let signed_tx = Transaction::V0(transaction).sign(&Signer::InMemory(signer_clone));
            let tx_hash = format!("{:?}", signed_tx.get_hash());

            let request = methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: signed_tx,
            };

            let _response = client.call(request).await?;
            Ok(tx_hash)
        })
    }
}

// ── Contract calls ──────────────────────────────────────────────────────────

fn get_pending_ids(rpc: &Rpc, contract: &str) -> Result<Vec<u64>> {
    let args = serde_json::to_vec(&serde_json::json!({"from_index": 0, "limit": 10}))?;
    let bytes = rpc.view(contract, "get_pending_request_ids", &args)?;
    if bytes.is_empty() { return Ok(vec![]); }
    Ok(serde_json::from_slice(&bytes)?)
}

struct RequestInfo {
    input: String,
    max_instructions: u64,
    max_memory_mb: u32,
    max_execution_seconds: u64,
}

fn get_request_info(rpc: &Rpc, contract: &str, request_id: u64) -> Result<RequestInfo> {
    let args = serde_json::to_vec(&serde_json::json!({"request_id": request_id}))?;
    let bytes = rpc.view(contract, "get_request", &args)?;
    if bytes.is_empty() { anyhow::bail!("request {} not found", request_id); }
    let req: serde_json::Value = serde_json::from_slice(&bytes)?;
    let input_b64 = req.get("input_data").and_then(|v| v.as_str()).unwrap_or("");
    let decoded = base64::engine::general_purpose::STANDARD.decode(input_b64).unwrap_or_default();
    let limits = req.get("resource_limits");
    Ok(RequestInfo {
        input: String::from_utf8_lossy(&decoded).to_string(),
        max_instructions: limits.and_then(|l| l.get("max_instructions")).and_then(|v| v.as_u64()).unwrap_or(10_000_000_000),
        max_memory_mb: limits.and_then(|l| l.get("max_memory_mb")).and_then(|v| v.as_u64()).unwrap_or(256) as u32,
        max_execution_seconds: limits.and_then(|l| l.get("max_execution_seconds")).and_then(|v| v.as_u64()).unwrap_or(60),
    })
}

fn resolve(
    rpc: &Rpc, signer: &InMemorySigner, contract: &str,
    request_id: u64, success: bool, output: &str, time_ms: u64, instructions: u64,
) -> Result<String> {
    let args = serde_json::json!({
        "request_id": request_id,
        "response": {
            "success": success,
            "output": {"Text": output},
            "error": if success { serde_json::Value::Null } else { serde_json::Value::String("Execution failed".into()) },
            "resources_used": {"instructions": instructions, "time_ms": time_ms},
            "compilation_note": null,
            "refund_usd": null,
        }
    });
    rpc.send_tx(signer, contract, "resolve_execution", args, 100_000_000_000_000, 0)
}

// ── WASM discovery & execution ──────────────────────────────────────────────

fn find_wasm(search_dirs: &[String]) -> Option<PathBuf> {
    for dir in search_dirs {
        let base = PathBuf::from(dir);
        if !base.exists() { continue; }
        for name in &["nostr-identity", "near-signer-tee"] {
            let release = base.join(name)
                .join("target").join("wasm32-wasip2").join("release");
            if let Ok(entries) = release.read_dir() {
                for f in entries.flatten() {
                    let s = f.file_name().to_string_lossy().to_string();
                    if s.ends_with(".wasm") && !s.starts_with('.') && !s.contains("-deps") {
                        return Some(f.path());
                    }
                }
            }
        }
    }
    None
}

fn execute_wasm(wasm_path: &Path, input: &str, rpc_url: &str, env_vars: &HashMap<String, String>, req_limits: &RequestInfo) -> Result<(bool, String, u64, u64)> {

    let wasm_bytes = fs::read(wasm_path)
        .with_context(|| format!("reading {}", wasm_path.display()))?;

    let storage_dir = env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage"));
    fs::create_dir_all(&storage_dir).ok();

    let rpc_cfg = RpcProxyConfig {
        enabled: true,
        rpc_url: Some(rpc_url.to_string()),
        max_calls_per_execution: 100,
        allow_transactions: true,
    };
    let proxy = RpcProxy::new(rpc_cfg, rpc_url)?;

    let rt = tokio::runtime::Runtime::new()?;
    let handle = rt.handle().clone();

    let exec_ctx = ExecutionContext {
        outlayer_rpc: Some(Arc::new(proxy)),
        storage_config: None,
        runtime_handle: handle,
        compiled_cache: None,
        vrf_config: None,
        wallet_config: None,
    };

    let executor = Executor::new(req_limits.max_instructions, true).with_context(exec_ctx);

    let limits = ResourceLimits {
        max_instructions: req_limits.max_instructions,
        max_memory_mb: req_limits.max_memory_mb,
        max_execution_seconds: req_limits.max_execution_seconds,
    };

    let env = if env_vars.is_empty() { None } else { Some(env_vars.clone()) };

    let result = rt.block_on(executor.execute(
        &wasm_bytes, None, input.as_bytes(), &limits,
        env, Some("wasm32-wasip2"), &ResponseFormat::Text,
        None, None, None,
    ))?;

    let success = result.success;
    let time_ms = result.execution_time_ms;
    let instructions = result.instructions;
    let output = match &result.output {
        Some(ExecutionOutput::Text(t)) => t.clone(),
        Some(ExecutionOutput::Json(j)) => serde_json::to_string(j).unwrap_or_default(),
        Some(ExecutionOutput::Bytes(b)) => format!("{} bytes", b.len()),
        None => String::new(),
    };

    // Drop executor first (releases runtime handle), then runtime can drop cleanly
    drop(executor);
    drop(rt);
    Ok((success, output, time_ms, instructions))
}

// ── Dashboard HTTP API ──────────────────────────────────────────────────────

async fn api_status(State(state): State<Arc<DashboardState>>) -> axum::Json<DaemonStatus> {
    let inner = state.status.lock().unwrap();
    axum::Json(DaemonStatus {
        running: true,
        uptime_secs: inner.start_time.elapsed().as_secs(),
        poll_count: inner.poll_count,
        last_poll_time: inner.last_poll_time.clone(),
        contract_id: inner.contract_id.clone(),
        account_id: inner.account_id.clone(),
        rpc_url: inner.rpc_url.clone(),
        poll_interval_secs: inner.poll_interval_secs,
        dashboard_addr: inner.dashboard_addr.clone(),
    })
}

async fn api_history(State(state): State<Arc<DashboardState>>) -> axum::Json<Vec<ExecutionRecord>> {
    let hist = state.history.lock().unwrap();
    let mut records: Vec<ExecutionRecord> = hist.iter().rev().take(50).cloned().collect();
    records.reverse();
    axum::Json(records)
}

async fn api_stream(
    State(state): State<Arc<DashboardState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.events_tx.subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
    let stream = stream.filter_map(|result| {
        match result {
            Ok(msg) => Some(Ok(Event::default().data(msg))),
            Err(_) => None,
        }
    });
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

#[derive(Serialize)]
struct StorageEntry {
    name: String,
    hex_name: String,
    size: u64,
}

async fn api_storage(State(state): State<Arc<DashboardState>>) -> axum::Json<Vec<StorageEntry>> {
    let dir = &state.storage_dir;
    let mut entries = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let hex_name = e.file_name().to_string_lossy().to_string();
            let decoded = hex::decode(&hex_name)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .unwrap_or_else(|| hex_name.clone());
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            entries.push(StorageEntry { name: decoded, hex_name, size });
        }
    }
    axum::Json(entries)
}

#[derive(Serialize)]
struct ContractState {
    pending_request_ids: Vec<u64>,
    pending_count: usize,
    contract_id: String,
}

async fn api_contract(State(state): State<Arc<DashboardState>>) -> axum::Json<ContractState> {
    // Use a blocking call to query the contract
    let rpc_url = state.rpc_url.clone();
    let contract_id = state.contract_id.clone();
    let result = std::thread::spawn(move || -> Result<Vec<u64>> {
        let rpc = Rpc::new(&rpc_url)?;
        get_pending_ids(&rpc, &contract_id)
    }).join().unwrap_or(Ok(vec![]));

    let ids = result.unwrap_or_default();
    axum::Json(ContractState {
        pending_count: ids.len(),
        pending_request_ids: ids,
        contract_id: state.contract_id.clone(),
    })
}

fn spawn_dashboard(addr: &str, state: Arc<DashboardState>) {
    let addr_str = addr.to_string();
    let addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => { eprintln!("❌ Invalid dashboard address '{}': {}", addr, e); return; }
    };
    let state_clone = state.clone();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => { eprintln!("❌ Dashboard runtime failed: {}", e); return; }
        };
        rt.block_on(async move {
            let app = Router::new()
                .route("/api/status", get(api_status))
                .route("/api/history", get(api_history))
                .route("/api/stream", get(api_stream))
                .route("/api/storage", get(api_storage))
                .route("/api/contract", get(api_contract))
                .layer(CorsLayer::permissive())
                .with_state(state_clone);

            eprintln!("📊 Dashboard: http://{}", addr_str);
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => { eprintln!("❌ Dashboard bind failed: {}", e); return; }
            };
            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("❌ Dashboard server error: {}", e);
            }
        });
    });
}

// ── WebSocket subscription ──────────────────────────────────────────────────

fn rpc_to_ws_url(rpc_url: &str) -> String {
    let url = rpc_url
        .replace("https://", "wss://")
        .replace("http://", "ws://");
    if url.ends_with("/ws") { url } else { format!("{}/ws", url.trim_end_matches('/')) }
}

fn websocket_worker(
    cfg: &Config,
    rpc: &Rpc,
    signer: &InMemorySigner,
    dashboard_state: &Arc<DashboardState>,
    processed: &mut HashSet<u64>,
    log: &mut dyn FnMut(&str),
) -> Result<()> {
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    use futures_util::{SinkExt, StreamExt};

    let ws_url = rpc_to_ws_url(&cfg.rpc_url);
    log(&format!("🔗 Connecting to WS: {}", ws_url));

    let rt = tokio::runtime::Runtime::new()?;
    let mut consecutive_ws_errors = 0u32;

    'reconnect: loop {
        let connect_result = rt.block_on(async {
            match connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();
                    // Subscribe to finalized blocks
                    let sub_msg = r#"{"jsonrpc":"2.0","id":1,"method":"subscribe","params":["block_finalized"]}"#;
                    write.send(Message::Text(sub_msg.into())).await.map_err(|e| anyhow::anyhow!("{}", e))?;
                    Ok((write, read))
                }
                Err(e) => Err(anyhow::anyhow!("WS connect failed: {}", e)),
            }
        });

        let (mut write, mut read) = match connect_result {
            Ok(pair) => {
                consecutive_ws_errors = 0;
                log(&format!("✅ WS connected, subscribed to block_finalized"));
                pair
            }
            Err(e) => {
                consecutive_ws_errors += 1;
                let backoff = std::cmp::min(2u64.pow(std::cmp::min(consecutive_ws_errors, 8)), 120);
                log(&format!("❌ {} (retry in {}s, attempt #{})", e, backoff, consecutive_ws_errors));
                if consecutive_ws_errors >= 10 {
                    log("❌ WS failed too many times, falling back to poll mode");
                    return Err(anyhow::anyhow!("WS fallback to poll"));
                }
                std::thread::sleep(Duration::from_secs(backoff));
                continue 'reconnect;
            }
        };

        // Read messages
        loop {
            let msg_result = rt.block_on(async { futures_util::StreamExt::next(&mut read).await });
            match msg_result {
                Some(Ok(Message::Text(text))) => {
                    // Check if it's a block notification
                    if text.contains("block_finalized") || text.contains("block") {
                        // Update poll count
                        {
                            let mut st = dashboard_state.status.lock().unwrap();
                            st.poll_count += 1;
                            st.last_poll_time = Some(now());
                        }

                        // Check pending requests
                        match get_pending_ids(&rpc, &cfg.contract_id) {
                            Ok(ids) => {
                                if !ids.is_empty() {
                                    log(&format!("Pending: {:?}", ids));
                                    for req_id in &ids {
                                        if processed.contains(req_id) { continue; }
                                        process_request(req_id, &rpc, &cfg, &signer, &dashboard_state, processed, log);
                                    }
                                }
                            }
                            Err(e) => log(&format!("❌ RPC error: {}", e)),
                        }
                    }
                }
                Some(Ok(Message::Ping(data))) => {
                    let _ = rt.block_on(async { write.send(Message::Pong(data)).await });
                }
                Some(Ok(Message::Close(_))) | None => {
                    log("⚠️ WS disconnected, reconnecting...");
                    std::thread::sleep(Duration::from_secs(2));
                    continue 'reconnect;
                }
                _ => {}
            }
        }
    }
}

fn process_request(
    req_id: &u64,
    rpc: &Rpc,
    cfg: &Config,
    signer: &InMemorySigner,
    dashboard_state: &Arc<DashboardState>,
    processed: &mut HashSet<u64>,
    log: &mut dyn FnMut(&str),
) {
    log(&format!("📋 Request #{}", req_id));

    let req_info = match get_request_info(&rpc, &cfg.contract_id, *req_id) {
        Ok(i) => i,
        Err(e) => { log(&format!("   ❌ {}", e)); return; }
    };
    log(&format!("   Input: {}", req_info.input));

    let wasm = match find_wasm(&cfg.wasm_search_dirs) {
        Some(w) => w,
        None => { log("   ❌ WASM not found"); return; }
    };
    log(&format!("   WASM: {}", wasm.display()));

    log("   🏃 Running...");
    match execute_wasm(&wasm, &req_info.input, &cfg.rpc_url, &cfg.env, &req_info) {
        Ok((success, output, time_ms, instructions)) => {
            log(&format!("   ✅ {} | {}ms | {} instr", success, time_ms, instructions));
            log(&format!("   📤 {}", output));

            let record = ExecutionRecord {
                request_id: *req_id,
                input: req_info.input.clone(),
                output: output.clone(),
                execution_time_ms: time_ms,
                instructions,
                timestamp: now(),
                success,
            };
            {
                let mut hist = dashboard_state.history.lock().unwrap();
                hist.push(record);
                if hist.len() > 200 { let e = hist.len().saturating_sub(200); hist.drain(0..e); }
            }

            match resolve(&rpc, &signer, &cfg.contract_id, *req_id, success, &output, time_ms, instructions) {
                Ok(tx_hash) => log(&format!("   ✅ Tx: {}", tx_hash)),
                Err(e) => log(&format!("   ❌ Submit failed: {}", e)),
            }
        }
        Err(e) => {
            log(&format!("   ❌ {}", e));
            let record = ExecutionRecord {
                request_id: *req_id,
                input: req_info.input.clone(),
                output: format!("Error: {}", e),
                execution_time_ms: 0,
                instructions: 0,
                timestamp: now(),
                success: false,
            };
            {
                let mut hist = dashboard_state.history.lock().unwrap();
                hist.push(record);
                if hist.len() > 200 { let e = hist.len().saturating_sub(200); hist.drain(0..e); }
            }
            let _ = resolve(&rpc, &signer, &cfg.contract_id, *req_id, false, "", 0, 0);
        }
    }
    processed.insert(*req_id);
}

// ── Main ────────────────────────────────────────────────────────────────────

fn usage() {
    eprintln!("layerd v{} — OutLayer local worker", env!("CARGO_PKG_VERSION"));
    eprintln!();
    eprintln!("Usage: layerd [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --daemon            Run as background daemon");
    eprintln!("  --foreground        Run in foreground (default)");
    eprintln!("  --dashboard <addr>  Enable dashboard HTTP server (e.g. 127.0.0.1:8082)");
    eprintln!("  --start             Start daemon via launchd");
    eprintln!("  --stop              Stop daemon (unloads launchd)");
    eprintln!("  --status            Check daemon status");
    eprintln!("  --log               Tail the log file");
    eprintln!("  -h, --help          Show this help");
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let cfg = Config::load();

    // Parse --dashboard flag from CLI (overrides config)
    let dashboard_addr = parse_dashboard_flag(&args).or(cfg.dashboard_addr.clone());

    // Handle CLI flags
    let mode = if args.iter().any(|a| a == "--stop") {
        return stop_daemon(&cfg);
    } else if args.iter().any(|a| a == "--start") {
        return start_daemon(&cfg);
    } else if args.iter().any(|a| a == "--status") {
        return check_status(&cfg);
    } else if args.iter().any(|a| a == "--log") {
        return tail_log(&cfg);
    } else if args.iter().any(|a| a == "-h" || a == "--help") {
        usage();
        return Ok(());
    } else if args.iter().any(|a| a == "--daemon") {
        "daemon"
    } else {
        "foreground"
    };

    if mode == "daemon" {
        let pid_path = cfg.pid_file_path();
        let log_path = cfg.log_file_path();

        // Check if already running
        if is_running(&pid_path) {
            eprintln!("layerd already running (PID {})", read_pid(&pid_path).unwrap_or_default());
            std::process::exit(1);
        }

        eprintln!("⚡ Starting layerd daemon...");
        eprintln!("   Log: {}", log_path.display());
        eprintln!("   PID: {}", pid_path.display());

        daemonize(&log_path, &pid_path)?;
    } else {
        // Write PID file for --status/--stop to work
        let pid_path = cfg.pid_file_path();
        if let Some(parent) = pid_path.parent() { fs::create_dir_all(parent).ok(); }
        fs::write(&pid_path, std::process::id().to_string()).ok();

        eprintln!("⚡ layerd — OutLayer local worker (direct RPC)");
        eprintln!("   Contract: {}", cfg.contract_id);
        eprintln!("   Account:  {}", cfg.account_id);
        eprintln!("   RPC:      {}", cfg.rpc_url);
        eprintln!("   Poll:     {}s", cfg.poll_interval_secs);
    }

    // ── Dashboard setup ────────────────────────────────────────────────
    let (events_tx, _) = broadcast::channel(100);
    let storage_dir = env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage"));

    let dashboard_state = Arc::new(DashboardState {
        history: Mutex::new(Vec::new()),
        status: Mutex::new(DashboardStatusInner {
            start_time: std::time::Instant::now(),
            poll_count: 0,
            last_poll_time: None,
            contract_id: cfg.contract_id.clone(),
            account_id: cfg.account_id.clone(),
            rpc_url: cfg.rpc_url.clone(),
            poll_interval_secs: cfg.poll_interval_secs,
            dashboard_addr: dashboard_addr.clone(),
        }),
        events_tx,
        storage_dir,
        contract_id: cfg.contract_id.clone(),
        rpc_url: cfg.rpc_url.clone(),
    });

    if let Some(ref addr) = dashboard_addr {
        spawn_dashboard(addr, dashboard_state.clone());
    }

    // ── Worker loop ─────────────────────────────────────────────────────
    let signer = load_signer(&cfg.key_path)?;
    let is_daemon = mode == "daemon";
    let mut log_file = if is_daemon {
        let log_path = cfg.log_file_path();
        if let Some(parent) = log_path.parent() { fs::create_dir_all(parent).ok(); }
        fs::OpenOptions::new().create(true).append(true).open(&log_path).ok()
    } else {
        None
    };

    let mut log = |msg: &str| {
        let line = format!("{} {}\n", now(), msg);
        if is_daemon {
            if let Some(ref mut f) = log_file {
                let _ = f.write_all(line.as_bytes());
            }
        } else {
            eprint!("{}", line);
        }
        // Broadcast to SSE subscribers
        let _ = dashboard_state.events_tx.send(msg.to_string());
    };

    log(&format!("⚡ layerd started — Contract: {} Account: {} RPC: {} PollMode: {}",
        cfg.contract_id, cfg.account_id, cfg.rpc_url, cfg.poll_mode));

    let rpc = Rpc::new(&cfg.rpc_url)?;
    let mut processed: HashSet<u64> = HashSet::new();

    // Clean up PID file on exit
    let pid_path_cleanup = cfg.pid_file_path();
    ctrlc_handler(&pid_path_cleanup);

    // ── WebSocket mode ──────────────────────────────────────────────
    if cfg.poll_mode == "websocket" {
        let ws_result = websocket_worker(&cfg, &rpc, &signer, &dashboard_state, &mut processed, &mut log);
        if ws_result.is_err() {
            log("⚠️ Falling back to poll mode...");
        }
        // If websocket_worker returns (fallback), continue to poll loop below
    }

    // ── Poll mode ───────────────────────────────────────────────────
    let mut consecutive_errors = 0u32;

    loop {
        // Update poll count
        {
            let mut st = dashboard_state.status.lock().unwrap();
            st.poll_count += 1;
            st.last_poll_time = Some(now());
        }

        match get_pending_ids(&rpc, &cfg.contract_id) {
            Ok(ids) => {
                consecutive_errors = 0;
                if ids.is_empty() {
                    // silent — nothing to do
                } else {
                    log(&format!("Pending: {:?}", ids));
                    for req_id in &ids {
                        if processed.contains(req_id) { continue; }
                        process_request(req_id, &rpc, &cfg, &signer, &dashboard_state, &mut processed, &mut log);
                    }
                }
            }
            Err(e) => {
                consecutive_errors += 1;
                let backoff = std::cmp::min(
                    cfg.poll_interval_secs * (1 << std::cmp::min(consecutive_errors, 5)),
                    300, // max 5 min backoff
                );
                log(&format!("❌ {} (backoff {}s, attempt #{})", e, backoff, consecutive_errors));
                std::thread::sleep(Duration::from_secs(backoff));
                continue;
            }
        }

        std::thread::sleep(Duration::from_secs(cfg.poll_interval_secs));
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn parse_dashboard_flag(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == "--dashboard" && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

// ── Daemon management ───────────────────────────────────────────────────────

fn is_running(pid_path: &Path) -> bool {
    if let Ok(pid_str) = fs::read_to_string(pid_path) {
        if let Ok(pid) = pid_str.trim().parse::<u32>() {
            // Send signal 0 to check if process exists
            unsafe {
                return libc::kill(pid as i32, 0) == 0;
            }
        }
    }
    false
}

fn read_pid(pid_path: &Path) -> Result<u32> {
    let s = fs::read_to_string(pid_path)?;
    Ok(s.trim().parse()?)
}

fn ctrlc_handler(_pid_path: &Path) {
    // Stale PID files are handled by is_running() which checks actual process
    // Just a marker — no complex signal handling needed
}

fn start_daemon(_cfg: &Config) -> Result<()> {
    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());

    if let Some(plist_path) = &plist {
        let status = std::process::Command::new("launchctl")
            .args(["load", &plist_path.display().to_string()])
            .status()?;
        if status.success() {
            eprintln!("✅ layerd started via launchd");
        } else {
            anyhow::bail!("launchctl load failed");
        }
    } else {
        anyhow::bail!("launchd plist not found at ~/Library/LaunchAgents/com.outlayer.layerd.plist");
    }
    Ok(())
}

fn stop_daemon(cfg: &Config) -> Result<()> {
    let pid_path = cfg.pid_file_path();

    // Try launchd unload first (macOS)
    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());

    if let Some(plist_path) = &plist {
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &plist_path.display().to_string()])
            .status();
        std::thread::sleep(Duration::from_millis(500));
    }

    // Also kill directly if still running
    if is_running(&pid_path) {
        let pid = read_pid(&pid_path)?;
        let my_pid = std::process::id();
        if pid == my_pid {
            eprintln!("✅ Stopped via launchd");
            return Ok(());
        }
        eprintln!("Stopping layerd (PID {})...", pid);
        unsafe { libc::kill(pid as i32, libc::SIGTERM); }
        for _ in 0..10 {
            if !is_running(&pid_path) {
                let _ = fs::remove_file(&pid_path);
                eprintln!("✅ Stopped");
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        unsafe { libc::kill(pid as i32, libc::SIGKILL); }
        let _ = fs::remove_file(&pid_path);
        eprintln!("✅ Force killed");
    } else {
        let _ = fs::remove_file(&pid_path);
        eprintln!("✅ Stopped (was not running)");
    }
    Ok(())
}

fn check_status(cfg: &Config) -> Result<()> {
    let pid_path = cfg.pid_file_path();
    let log_path = cfg.log_file_path();
    if is_running(&pid_path) {
        let pid = read_pid(&pid_path)?;
        eprintln!("✅ layerd running (PID {})", pid);
        eprintln!("   Log: {}", log_path.display());
        eprintln!("   PID: {}", pid_path.display());
    } else {
        eprintln!("❌ layerd not running");
        if pid_path.exists() {
            eprintln!("   (stale PID file, cleaning up)");
            let _ = fs::remove_file(&pid_path);
        }
    }
    Ok(())
}

fn tail_log(cfg: &Config) -> Result<()> {
    let log_path = cfg.log_file_path();
    if !log_path.exists() {
        eprintln!("No log file at {}", log_path.display());
        return Ok(());
    }
    let _ = std::process::Command::new("tail")
        .args(["-20", &log_path.display().to_string()])
        .status()?;
    Ok(())
}

fn now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

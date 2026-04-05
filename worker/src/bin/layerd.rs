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
use sha2::Digest;
use crossbeam_channel::Receiver;

use near_crypto::{InMemorySigner, Signer};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Transaction, TransactionV0};
use near_primitives::types::BlockReference;
use near_primitives::views::QueryRequest;

// Executor imports used in execute_wasm()
use offchainvm_worker::api_client::{ExecutionOutput, ResourceLimits, ResponseFormat};
use offchainvm_worker::config::RpcProxyConfig;
use offchainvm_worker::compiled_cache::CompiledCache;
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

// ── WASM & Engine caching (Optimization #2) ─────────────────────────────────

use std::sync::OnceLock;

/// Cached WASM file bytes — read once, reused across all executions.
static WASM_BYTES_CACHE: OnceLock<Vec<u8>> = OnceLock::new();

/// Cached WASM path — so we don't re-discover every tick.
static WASM_PATH_CACHE: OnceLock<PathBuf> = OnceLock::new();

/// Shared tokio runtime for all WASM executions — avoids creating one per request.
static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn shared_runtime() -> &'static tokio::runtime::Runtime {
    SHARED_RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to create shared runtime"))
}

/// Shared compiled WASM cache — avoids re-JIT on every execution.
static COMPILED_CACHE: OnceLock<Arc<std::sync::Mutex<CompiledCache>>> = OnceLock::new();

fn init_compiled_cache(secret_key_bytes: [u8; 32]) {
    let home = dirs::home_dir().unwrap_or_default();
    let cache_dir = home.join(".inlayer").join("compiled_cache");
    match CompiledCache::new(cache_dir, 500, &secret_key_bytes) {
        Ok(cache) => { COMPILED_CACHE.set(Arc::new(std::sync::Mutex::new(cache))).ok(); }
        Err(e) => eprintln!("⚠️ Compiled cache init failed: {}", e),
    }
}

fn compiled_cache() -> Option<Arc<std::sync::Mutex<CompiledCache>>> {
    COMPILED_CACHE.get().cloned()
}

/// Derive a 32-byte key from the signer's secret key for compiled cache encryption.
fn signer_key_bytes(signer: &InMemorySigner) -> [u8; 32] {
    use sha2::Sha256;
    // near_crypto::SecretKey wraps ed25519_dalek::SigningKey
    let sk_str = signer.secret_key.to_string(); // "ed25519:base64..."
    let b64 = sk_str.strip_prefix("ed25519:").unwrap_or(&sk_str);
    let sk_bytes = base64::engine::general_purpose::STANDARD.decode(b64).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&sk_bytes);
    hasher.finalize().into()
}

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
    last_block_height: Option<u64>,
    contract_id: String,
    account_id: String,
    rpc_url: String,
    poll_interval_secs: u64,
    dashboard_addr: Option<String>,
}

// ── Block Watcher (neardata.xyz event-driven polling) ───────────────────────

/// Derives the neardata.xyz base URL from the network name.
fn neardata_base_url(network: &str) -> String {
    match network {
        "mainnet" => "https://neardata.xyz".to_string(),
        _ => format!("https://{}.neardata.xyz", network),
    }
}

/// Discovers the latest finalized block height from neardata.xyz.
/// Returns None on any error (caller falls back to RPC).
fn discover_neardata_height(base_url: &str) -> Option<u64> {
    let url = format!("{}/v0/last_block/final", base_url);
    let resp = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?
        .get(&url)
        .send()
        .ok()?;

    // neardata redirects /v0/last_block/final → /v0/block/<height>
    let final_url = resp.url();
    final_url.path_segments()
        .and_then(|mut s| s.next_back())
        .and_then(|s: &str| s.parse().ok())
}

/// Spawns a background thread that watches for new blocks via neardata.xyz.
/// Sends the new block height on the channel whenever a new block is detected.
/// Falls back to periodic ticks if neardata is unavailable.
fn spawn_block_watcher(network: &str, poll_interval_secs: u64) -> Receiver<u64> {
    let (tx, rx) = crossbeam_channel::bounded(16);
    let base_url = neardata_base_url(network);

    std::thread::Builder::new()
        .name("block-watcher".into())
        .spawn(move || {
            let mut last_height: Option<u64> = None;
            let mut neardata_failures: u32 = 0;

            loop {
                // Try neardata.xyz first
                let discovered = discover_neardata_height(&base_url);

                match discovered {
                    Some(height) => {
                        neardata_failures = 0;
                        if last_height != Some(height) {
                            last_height = Some(height);
                            if tx.send(height).is_err() {
                                break; // Receiver dropped, exit
                            }
                        }
                        // neardata works — poll every ~600ms (NEAR block time ~1s)
                        std::thread::sleep(Duration::from_millis(600));
                    }
                    None => {
                        neardata_failures += 1;
                        // Fallback: send a tick anyway so the main loop can use RPC
                        if tx.send(0).is_err() {
                            break;
                        }
                        let backoff = if neardata_failures > 10 {
                            std::cmp::min(poll_interval_secs * 2, 120)
                        } else {
                            poll_interval_secs
                        };
                        std::thread::sleep(Duration::from_secs(backoff));
                    }
                }
            }
        })
        .expect("failed to spawn block watcher thread");

    rx
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
        }
    }
}

impl Config {
    fn load() -> Self {
        // Check for explicit --config flag
        let args: Vec<String> = env::args().collect();
        for i in 0..args.len() {
            if args[i] == "--config" && i + 1 < args.len() {
                if let Ok(s) = std::fs::read_to_string(&args[i + 1]) {
                    if let Ok(mut cfg) = toml::from_str::<Config>(&s) {
                        cfg.expand_tildes();
                        return cfg;
                    }
                }
            }
        }

        // Search standard locations
        for dir in &[".", &dirs::home_dir().unwrap_or_default().join(".inlayer").display().to_string()] {
            for name in &["inlayer.config", "inlayer.config.toml", "layerd.config", "layerd.config.toml"] {
                let path = PathBuf::from(dir).join(name);
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(mut cfg) = toml::from_str::<Config>(&s) {
                        cfg.expand_tildes();
                        return cfg;
                    }
                }
            }
        }
        Config::default()
    }

    /// Expand ~ in paths to home directory
    fn expand_tildes(&mut self) {
        let home = dirs::home_dir().unwrap_or_default();
        let home_str = home.display().to_string();
        for dir in &mut self.wasm_search_dirs {
            if dir.starts_with("~/") {
                *dir = format!("{}/{}", home_str, &dir[2..]);
            }
        }
        if self.key_path.starts_with("~/") {
            self.key_path = format!("{}/{}", home_str, &self.key_path[2..]);
        }
        if let Some(ref mut p) = self.log_file {
            if p.starts_with("~/") {
                *p = format!("{}/{}", home_str, &p[2..]);
            }
        }
        if let Some(ref mut p) = self.pid_file {
            if p.starts_with("~/") {
                *p = format!("{}/{}", home_str, &p[2..]);
            }
        }
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
            let dn = fs::File::open("/dev/null").ok();
            let dn_fd = dn.as_ref().map(|f| f.as_raw_fd()).unwrap_or(-1);
            unsafe {
                libc::dup(dn_fd);
                libc::dup(dn_fd);
                libc::dup(dn_fd);
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

    /// Fetch multiple request infos concurrently (Optimization #5)
    fn fetch_request_infos(&self, contract: &str, ids: &[u64]) -> Vec<(u64, Result<RequestInfo>)> {
        let client = self.client.clone();
        let contract_id = contract.to_string();
        self.rt.block_on(async {
            let futures: Vec<_> = ids.iter().map(|&req_id| {
                let client = client.clone();
                let contract_id = contract_id.clone();
                async move {
                    let result = Self::view_async(&client, &contract_id, "get_request", serde_json::json!({"request_id": req_id}).to_string().into_bytes()).await;
                    let parsed = result.and_then(|bytes| {
                        if bytes.is_empty() { anyhow::bail!("request {} not found", req_id); }
                        let req: serde_json::Value = serde_json::from_slice(&bytes)?;
                        let input_raw = req.get("input_data").and_then(|v| v.as_str()).unwrap_or("");
                        // input_data is plain JSON string, not base64
                        let input_str = if input_raw.is_empty() {
                            String::new()
                        } else {
                            // Try base64 decode first, fall back to raw string
                            match base64::engine::general_purpose::STANDARD.decode(input_raw) {
                                Ok(decoded) if !decoded.is_empty() => String::from_utf8_lossy(&decoded).to_string(),
                                _ => input_raw.to_string(),
                            }
                        };                        let limits = req.get("resource_limits");
                        Ok(RequestInfo {
                            input: input_str,
                            max_instructions: limits.and_then(|l| l.get("max_instructions")).and_then(|v| v.as_u64()).unwrap_or(10_000_000_000),
                            max_memory_mb: limits.and_then(|l| l.get("max_memory_mb")).and_then(|v| v.as_u64()).unwrap_or(256) as u32,
                            max_execution_seconds: limits.and_then(|l| l.get("max_execution_seconds")).and_then(|v| v.as_u64()).unwrap_or(60),
                        })
                    });
                    (req_id, parsed)
                }
            }).collect();
            futures::future::join_all(futures).await
        })
    }

    async fn view_async(client: &JsonRpcClient, contract: &str, method: &str, args: Vec<u8>) -> Result<Vec<u8>> {
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

/// Fetch a fresh nonce and block hash from the RPC.
fn fetch_nonce_block(rpc_url: &str, signer: &InMemorySigner) -> Result<(u64, CryptoHash)> {
    let client = JsonRpcClient::connect(rpc_url);
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let query = methods::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccessKey {
                account_id: signer.account_id.clone(),
                public_key: signer.public_key.clone(),
            },
        };
        let response = client.call(query).await?;
        let nonce = match response.kind {
            QueryResponseKind::AccessKey(ak) => ak.nonce + 1,
            _ => anyhow::bail!("unexpected query response for access key"),
        };
        Ok((nonce, response.block_hash))
    })
}

/// Cached nonce tracker — avoids re-fetching from RPC every batch.
/// Auto-increments locally; re-fetches on InvalidNonce errors.
struct NonceCache {
    inner: std::sync::Mutex<NonceCacheInner>,
    rpc_url: String,
    signer: InMemorySigner,
}

struct NonceCacheInner {
    nonce: Option<u64>,
    block_hash: Option<CryptoHash>,
}

impl NonceCache {
    fn new(rpc_url: String, signer: InMemorySigner) -> Self {
        Self {
            inner: std::sync::Mutex::new(NonceCacheInner { nonce: None, block_hash: None }),
            rpc_url,
            signer,
        }
    }

    /// Get N sequential nonces. Fetches from RPC on first call or after invalidation.
    fn reserve_batch(&self, count: usize) -> Result<(u64, CryptoHash)> {
        let mut inner = self.inner.lock().unwrap();
        if inner.nonce.is_none() {
            drop(inner);
            let (nonce, hash) = fetch_nonce_block(&self.rpc_url, &self.signer)?;
            inner = self.inner.lock().unwrap();
            inner.nonce = Some(nonce);
            inner.block_hash = Some(hash);
        }
        let base = inner.nonce.unwrap();
        let hash = inner.block_hash.unwrap();
        inner.nonce = Some(base + count as u64);
        Ok((base, hash))
    }

    /// Invalidate cache — next reserve_batch will re-fetch from RPC.
    fn invalidate(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.nonce = None;
        inner.block_hash = None;
    }

    /// Inject a pre-fetched nonce into the cache if it's empty.
    fn prefill(&self, nonce: u64, block_hash: CryptoHash) {
        let mut inner = self.inner.lock().unwrap();
        if inner.nonce.is_none() {
            inner.nonce = Some(nonce);
            inner.block_hash = Some(block_hash);
        }
    }
}

/// Submit a single resolve tx with given nonce/block_hash.
fn resolve_one(
    rpc_url: &str, signer: &InMemorySigner, contract: &str,
    req_id: u64, success: bool, output: &str, time_ms: u64, instructions: u64,
    nonce: u64, block_hash: CryptoHash,
) -> Result<String> {
    let args = serde_json::json!({
        "request_id": req_id,
        "response": {
            "success": success,
            "output": {"Text": output},
            "error": if success { serde_json::Value::Null } else { serde_json::Value::String("Execution failed".into()) },
            "resources_used": {"instructions": instructions, "time_ms": time_ms},
            "compilation_note": null,
            "refund_usd": null,
        }
    });

    let client = JsonRpcClient::connect(rpc_url);
    let rt = tokio::runtime::Runtime::new()?;

    let signer_account_id = signer.account_id.clone();
    let signer_public_key = signer.public_key.clone();
    let signer_clone = signer.clone();
    let contract_id: near_primitives::types::AccountId = contract.parse()?;
    let method_name = "resolve_execution".to_string();
    let args_bytes = serde_json::to_vec(&args)?;

    rt.block_on(async {
        let transaction = TransactionV0 {
            signer_id: signer_account_id,
            public_key: signer_public_key,
            nonce,
            receiver_id: contract_id,
            block_hash,
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name,
                args: args_bytes,
                gas: 100_000_000_000_000,
                deposit: 0,
            }))],
        };

        let signed_tx = Transaction::V0(transaction).sign(&Signer::InMemory(signer_clone));
        let tx_hash = format!("{:?}", signed_tx.get_hash());

        // Use send_tx (modern replacement for broadcast_tx_commit)
        // wait_until EXECUTED_OPTIMISTIC = included + receipts executed (same as old commit)
        client.call(methods::send_tx::RpcSendTransactionRequest {
            signed_transaction: signed_tx,
            wait_until: near_primitives::views::TxExecutionStatus::ExecutedOptimistic,
        }).await.map_err(|e| anyhow::anyhow!("send_tx failed: {}", e))?;

        Ok(tx_hash)
    })
}

/// Batch-resolve via single batch_resolve_execution contract call.
/// Sends all results in one transaction instead of N separate ones.
fn resolve_batch(
    nonce_cache: &NonceCache, signer: &InMemorySigner, contract: &str,
    payloads: Vec<(u64, bool, String, u64, u64)>,
) -> Vec<(u64, Result<String>)> {
    if payloads.is_empty() {
        return Vec::new();
    }

    // If only 1 request, use individual resolve with InvalidNonce retry
    if payloads.len() == 1 {
        let (req_id, success, output, time_ms, instructions) = &payloads[0];
        for attempt in 0..2 {
            let (base_nonce, block_hash) = match nonce_cache.reserve_batch(1) {
                Ok(r) => r,
                Err(e) => return vec![(*req_id, Err(anyhow::anyhow!("nonce fetch failed: {}", e)))],
            };
            let result = resolve_one(&nonce_cache.rpc_url, signer, contract, *req_id, *success, output, *time_ms, *instructions, base_nonce, block_hash);
            match &result {
                Ok(_) => return vec![(*req_id, result)],
                Err(e) if e.to_string().contains("InvalidNonce") => {
                    nonce_cache.invalidate();
                    if attempt == 0 { continue; }
                }
                Err(_) => return vec![(*req_id, result)],
            }
        }
        return vec![(*req_id, Err(anyhow::anyhow!("nonce retry exhausted")))];
    }

    // Multiple requests → single batch_resolve_execution call
    let entries: Vec<serde_json::Value> = payloads.iter().map(|(req_id, success, output, time_ms, instructions)| {
        serde_json::json!([
            req_id,
            {
                "success": success,
                "output": {"Text": output},
                "error": if *success { serde_json::Value::Null } else { serde_json::Value::String("Execution failed".into()) },
                "resources_used": {"instructions": instructions, "time_ms": time_ms},
                "compilation_note": null,
                "refund_usd": null,
            }
        ])
    }).collect();

    let args = serde_json::json!({ "entries": entries });

    let rpc_url = &nonce_cache.rpc_url;
    let (base_nonce, block_hash) = match nonce_cache.reserve_batch(1) {
        Ok(r) => r,
        Err(e) => {
            nonce_cache.invalidate();
            return payloads.into_iter()
                .map(|(id, _, _, _, _)| (id, Err(anyhow::anyhow!("nonce fetch failed: {}", e))))
                .collect();
        }
    };

    let result = (|| -> Result<String> {
        let client = JsonRpcClient::connect(rpc_url);
        let rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async {
            let transaction = TransactionV0 {
                signer_id: signer.account_id.clone(),
                public_key: signer.public_key.clone(),
                nonce: base_nonce,
                receiver_id: contract.parse()?,
                block_hash,
                actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "batch_resolve_execution".to_string(),
                    args: serde_json::to_vec(&args)?,
                    gas: 100_000_000_000_000 * payloads.len() as u64,
                    deposit: 0,
                }))],
            };

            let signed_tx = Transaction::V0(transaction).sign(&Signer::InMemory(signer.clone()));
            let tx_hash = format!("{:?}", signed_tx.get_hash());

            client.call(methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: signed_tx,
            }).await.map_err(|e| anyhow::anyhow!("batch broadcast failed: {}", e))?;

            Ok(tx_hash)
        })
    })();

    // Map single result to all request IDs
    match result {
        Ok(tx_hash) => payloads.into_iter().map(|(id, _, _, _, _)| (id, Ok(tx_hash.clone()))).collect(),
        Err(e) => {
            nonce_cache.invalidate();
            payloads.into_iter().map(|(id, _, _, _, _)| (id, Err(anyhow::anyhow!("batch resolve failed: {}", e)))).collect()
        }
    }
}

// ── WASM discovery & execution ──────────────────────────────────────────────

/// Find the smallest .wasm file (Optimization #6 — prefer optimized binaries).
/// Caches the result after first successful lookup.
fn find_wasm(search_dirs: &[String]) -> Option<PathBuf> {
    // Return cached path if available
    if let Some(cached) = WASM_PATH_CACHE.get() {
        if cached.exists() {
            return Some(cached.clone());
        }
    }

    let mut best: Option<(PathBuf, u64)> = None;
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
                        let size = f.metadata().map(|m| m.len()).unwrap_or(u64::MAX);
                        let is_better = best.as_ref().map_or(true, |(_, sz)| size < *sz);
                        if is_better {
                            best = Some((f.path(), size));
                        }
                    }
                }
            }
        }
    }

    if let Some((path, size)) = best {
        // Warn if WASM is large (Optimization #6)
        if size > 1_000_000 {
            eprintln!("⚠️ WASM binary is {} bytes (>1MB). Consider running wasm-opt -Oz.", size);
        }
        let _ = WASM_PATH_CACHE.set(path.clone());
        Some(path)
    } else {
        None
    }
}

/// Get cached WASM bytes (Optimization #2 — read file once, cache forever)
fn get_wasm_bytes(wasm_path: &Path) -> Result<&'static [u8]> {
    if let Some(cached) = WASM_BYTES_CACHE.get() {
        // Verify the file hasn't changed (different path or modified)
        return Ok(cached);
    }
    let bytes = fs::read(wasm_path).with_context(|| format!("reading {}", wasm_path.display()))?;
    let _ = WASM_BYTES_CACHE.set(bytes);
    Ok(WASM_BYTES_CACHE.get().unwrap())
}

/// Result of a single WASM execution
struct WasmResult {
    request_id: u64,
    success: bool,
    output: String,
    time_ms: u64,
    instructions: u64,
    error: Option<String>,
    input: String,
}

/// Execute a single WASM request. Reads bytes from cache.
fn execute_single_wasm(
    wasm_bytes: &[u8],
    request_id: u64,
    input: &str,
    rpc_url: &str,
    env_vars: &HashMap<String, String>,
    req_limits: &RequestInfo,
) -> WasmResult {
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
    let proxy = match RpcProxy::new(rpc_cfg, rpc_url) {
        Ok(p) => p,
        Err(e) => return WasmResult {
            request_id,
            success: false,
            output: String::new(),
            time_ms: 0,
            instructions: 0,
            error: Some(format!("RPC proxy error: {}", e)),
            input: input.to_string(),
        },
    };

    // Use shared runtime instead of creating a new one per request
    let rt = shared_runtime();
    let handle = rt.handle().clone();

    let exec_ctx = ExecutionContext {
        outlayer_rpc: Some(Arc::new(proxy)),
        storage_config: None,
        runtime_handle: handle,
        compiled_cache: compiled_cache(),
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

    // Compute WASM checksum for compiled cache lookup
    let wasm_checksum = {
        use std::fmt::Write;
        let mut s = String::with_capacity(64);
        for &b in sha2::Sha256::digest(wasm_bytes).iter() {
            write!(&mut s, "{:02x}", b).unwrap();
        }
        s
    };

    let result = rt.block_on(executor.execute(
        wasm_bytes, Some(&wasm_checksum), input.as_bytes(), &limits,
        env, Some("wasm32-wasip2"), &ResponseFormat::Text,
        None, None, None,
    ));

    drop(executor);

    match result {
        Ok(r) => {
            let output = match &r.output {
                Some(ExecutionOutput::Text(t)) => t.clone(),
                Some(ExecutionOutput::Json(j)) => serde_json::to_string(j).unwrap_or_default(),
                Some(ExecutionOutput::Bytes(b)) => format!("{} bytes", b.len()),
                None => String::new(),
            };
            WasmResult {
                request_id,
                success: r.success,
                output,
                time_ms: r.execution_time_ms,
                instructions: r.instructions,
                error: r.error,
                input: input.to_string(),
            }
        }
        Err(e) => WasmResult {
            request_id,
            success: false,
            output: String::new(),
            time_ms: 0,
            instructions: 0,
            error: Some(e.to_string()),
            input: input.to_string(),
        },
    }
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

    let dashboard_addr = parse_dashboard_flag(&args).or(cfg.dashboard_addr.clone());

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

        if is_running(&pid_path) {
            eprintln!("layerd already running (PID {})", read_pid(&pid_path).unwrap_or_default());
            std::process::exit(1);
        }

        eprintln!("⚡ Starting layerd daemon...");
        eprintln!("   Log: {}", log_path.display());
        eprintln!("   PID: {}", pid_path.display());

        daemonize(&log_path, &pid_path)?;
    } else {
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
            last_block_height: None,
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
    init_compiled_cache(signer_key_bytes(&signer));
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
        let _ = dashboard_state.events_tx.send(msg.to_string());
    };

    log(&format!("⚡ layerd started — Contract: {} Account: {} RPC: {}",
        cfg.contract_id, cfg.account_id, cfg.rpc_url));

    let rpc = Rpc::new(&cfg.rpc_url)?;
    let mut processed: HashSet<u64> = HashSet::new();
    let nonce_cache = NonceCache::new(cfg.rpc_url.clone(), signer.clone());

    let pid_path_cleanup = cfg.pid_file_path();
    ctrlc_handler(&pid_path_cleanup);

    // ── Event-driven block watcher ──────────────────────────────────────
    // Spawns a background thread that monitors neardata.xyz for new blocks.
    // Sends block height on `block_rx` whenever a new block is detected.
    // Falls back to periodic ticks if neardata is unavailable.
    let block_rx = spawn_block_watcher(&cfg.network, cfg.poll_interval_secs);
    log("📡 Block watcher started (neardata.xyz event-driven polling)");

    let mut consecutive_errors = 0u32;

    loop {
        // Wait for next block notification from the watcher thread.
        // Height 0 = fallback tick (neardata unavailable), just poll via RPC.
        let watcher_height = match block_rx.recv_timeout(Duration::from_secs(cfg.poll_interval_secs * 3)) {
            Ok(h) => h,
            Err(_) => 0,
        };

        // Update dashboard with latest block height from watcher
        if watcher_height > 0 {
            let mut st = dashboard_state.status.lock().unwrap();
            st.last_block_height = Some(watcher_height);
        }

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
                    // No pending requests — wait for next block notification
                    continue;
                }

                log(&format!("Pending: {:?}", ids));

                // Filter out already-processed requests
                let unprocessed: Vec<u64> = ids.iter()
                    .filter(|id| !processed.contains(id))
                    .copied()
                    .collect();

                if unprocessed.is_empty() {
                    // All already processed — wait for next block notification
                    continue;
                }

                // Optimization: Fetch request infos (already concurrent)
                let infos = rpc.fetch_request_infos(&cfg.contract_id, &unprocessed);

                // Optimization: Pre-warm nonce cache while we set up WASM.
                // This fetches nonce from RPC in the background so resolve_batch
                // can use it immediately without waiting.
                let nonce_prefetch = std::thread::spawn({
                    let rpc_url = cfg.rpc_url.clone();
                    let signer_clone = signer.clone();
                    move || fetch_nonce_block(&rpc_url, &signer_clone)
                });

                if cfg.env.is_empty() {
                    log("⚠️ No env vars configured — WASM may not have NEAR_PRIVATE_KEY");
                }

                // Validate we have a WASM binary
                let wasm_path = match find_wasm(&cfg.wasm_search_dirs) {
                    Some(w) => w,
                    None => { log("   ❌ WASM not found"); continue; }
                };
                log(&format!("   WASM: {}", wasm_path.display()));

                // Optimization #2: Get cached WASM bytes
                let wasm_bytes = match get_wasm_bytes(&wasm_path) {
                    Ok(b) => b,
                    Err(e) => { log(&format!("   ❌ {}", e)); continue; }
                };

                // Optimization #1: Execute all WASMs in parallel
                let wasm_results: Vec<WasmResult> = std::thread::scope(|s| {
                    let handles: Vec<_> = infos.into_iter()
                        .filter_map(|(req_id, info_result)| {
                            match info_result {
                                Ok(info) => {
                                    log(&format!("📋 Request #{} — {}", req_id, info.input));
                                    let env = cfg.env.clone();
                                    let rpc_url = cfg.rpc_url.clone();
                                    Some(s.spawn(move || {
                                        execute_single_wasm(wasm_bytes, req_id, &info.input, &rpc_url, &env, &info)
                                    }))
                                }
                                Err(e) => {
                                    log(&format!("   ❌ Request #{} info failed: {}", req_id, e));
                                    None
                                }
                            }
                        })
                        .collect();

                    handles.into_iter()
                        .map(|h| h.join().unwrap())
                        .collect()
                });

                // Log results and prepare resolve payloads
                let resolve_payloads: Vec<(u64, bool, String, u64, u64)> = wasm_results.into_iter().map(|result| {
                    processed.insert(result.request_id);

                    let record = ExecutionRecord {
                        request_id: result.request_id,
                        input: result.input.clone(),
                        output: if result.success { result.output.clone() } else { result.error.clone().unwrap_or_default() },
                        execution_time_ms: result.time_ms,
                        instructions: result.instructions,
                        timestamp: now(),
                        success: result.success,
                    };
                    {
                        let mut hist = dashboard_state.history.lock().unwrap();
                        hist.push(record);
                        if hist.len() > 200 {
                            let excess = hist.len().saturating_sub(200); hist.drain(0..excess);
                        }
                    }

                    if result.success {
                        log(&format!("   ✅ #{} | {}ms | {} instr", result.request_id, result.time_ms, result.instructions));
                        log(&format!("   📤 {}", result.output));
                    } else {
                        let err = result.error.unwrap_or_default();
                        log(&format!("   ❌ #{}: {}", result.request_id, err));
                    }

                    let output = if result.success { result.output.clone() } else { String::new() };
                    (result.request_id, result.success, output, result.time_ms, result.instructions)
                }).collect();

                // Inject pre-fetched nonce into cache (pipelined with WASM exec)
                if let Ok(Ok((nonce, hash))) = nonce_prefetch.join() {
                    nonce_cache.prefill(nonce, hash);
                }

                // Submit resolve txs in parallel with cached nonces
                let resolve_results = resolve_batch(
                    &nonce_cache, &signer, &cfg.contract_id, resolve_payloads,
                );

                // Log tx results
                for (req_id, tx_result) in resolve_results {
                    match tx_result {
                        Ok(tx_hash) => log(&format!("   ✅ Tx: {}", tx_hash)),
                        Err(e) => log(&format!("   ❌ Submit #{} failed: {}", req_id, e)),
                    }
                }

                // Prune processed set — keep only IDs > max_seen - 1000 to prevent unbounded growth
                if processed.len() > 500 {
                    let max_id = processed.iter().max().copied().unwrap_or(0);
                    let min_keep = max_id.saturating_sub(1000);
                    processed.retain(|&id| id > min_keep);
                }

                // Optimization #4: Pipeline — immediately re-poll instead of sleeping
                // After processing, immediately wait for next block (no artificial delay)
                continue;
            }
            Err(e) => {
                consecutive_errors += 1;
                let backoff = std::cmp::min(
                    cfg.poll_interval_secs * (1 << std::cmp::min(consecutive_errors, 5)),
                    300,
                );
                log(&format!("❌ {} (backoff {}s, attempt #{})", e, backoff, consecutive_errors));
                std::thread::sleep(Duration::from_secs(backoff));
                continue;
            }
        }
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

    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());

    if let Some(plist_path) = &plist {
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &plist_path.display().to_string()])
            .status();
        std::thread::sleep(Duration::from_millis(500));
    }

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

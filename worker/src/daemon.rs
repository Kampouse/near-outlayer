//! Daemon mode — polls NEAR contract for pending execution requests,
//! executes WASM locally, and resolves results on-chain.
//!
//! Exposed as `inlayer daemon [--start|--stop|--status|--log|--daemon|--foreground|--dashboard <addr>]`.

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
use crossbeam_channel::Receiver;
use near_crypto::{InMemorySigner, Signer};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Transaction, TransactionV0};
use near_primitives::types::BlockReference;
use near_primitives::views::QueryRequest;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::os::unix::io::AsRawFd;
use std::sync::OnceLock;
use tokio::sync::broadcast;
use toml;

use crate::api_client::{ExecutionOutput, ResourceLimits, ResponseFormat};
use crate::compiled_cache::CompiledCache;
use crate::config::RpcProxyConfig;
use crate::executor::{ExecutionContext, Executor};
use crate::outlayer_rpc::RpcProxy;

// ── WASM & Engine caching ──────────────────────────────────────────────────

static WASM_BYTES_CACHE: OnceLock<Vec<u8>> = OnceLock::new();
static WASM_PATH_CACHE: OnceLock<PathBuf> = OnceLock::new();
static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn shared_runtime() -> &'static tokio::runtime::Runtime {
    SHARED_RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to create shared runtime"))
}

static COMPILED_CACHE: OnceLock<Arc<std::sync::Mutex<CompiledCache>>> = OnceLock::new();

fn init_compiled_cache(secret_key_bytes: [u8; 32]) {
    let home = dirs::home_dir().unwrap_or_default();
    let cache_dir = home.join(".inlayer").join("compiled_cache");
    match CompiledCache::new(cache_dir, 500, &secret_key_bytes) {
        Ok(cache) => { COMPILED_CACHE.set(Arc::new(std::sync::Mutex::new(cache))).ok(); }
        Err(e) => eprintln!("Compiled cache init failed: {}", e),
    }
}

fn compiled_cache() -> Option<Arc<std::sync::Mutex<CompiledCache>>> {
    COMPILED_CACHE.get().cloned()
}

fn signer_key_bytes(signer: &InMemorySigner) -> [u8; 32] {
    let sk_str = signer.secret_key.to_string();
    let b64 = sk_str.strip_prefix("ed25519:").unwrap_or(&sk_str);
    let sk_bytes = base64::engine::general_purpose::STANDARD.decode(b64).unwrap_or_default();
    let mut hasher = sha2::Sha256::new();
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
    history: std::sync::Mutex<Vec<ExecutionRecord>>,
    status: std::sync::Mutex<DashboardStatusInner>,
    events_tx: broadcast::Sender<String>,
    storage_dir: PathBuf,
    contract_id: String,
    rpc_url: String,
    search_paths: Vec<String>,
    env: HashMap<String, String>,
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

fn neardata_base_url(network: &str) -> String {
    match network {
        "mainnet" => "https://neardata.xyz".to_string(),
        _ => format!("https://{}.neardata.xyz", network),
    }
}

fn discover_neardata_height(base_url: &str) -> Option<u64> {
    let url = format!("{}/v0/last_block/final", base_url);
    let resp = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?
        .get(&url)
        .send()
        .ok()?;
    let final_url = resp.url();
    final_url.path_segments()
        .and_then(|mut s| s.next_back())
        .and_then(|s: &str| s.parse().ok())
}

fn spawn_block_watcher(network: &str, poll_interval_secs: u64) -> Receiver<u64> {
    let (tx, rx) = crossbeam_channel::bounded(16);
    let base_url = neardata_base_url(network);
    std::thread::Builder::new()
        .name("block-watcher".into())
        .spawn(move || {
            let mut last_height: Option<u64> = None;
            let mut neardata_failures: u32 = 0;
            loop {
                let discovered = discover_neardata_height(&base_url);
                match discovered {
                    Some(height) => {
                        neardata_failures = 0;
                        if last_height != Some(height) {
                            last_height = Some(height);
                            if tx.send(height).is_err() { break; }
                        }
                        std::thread::sleep(Duration::from_millis(600));
                    }
                    None => {
                        neardata_failures += 1;
                        if tx.send(0).is_err() { break; }
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

// ── RPC ─────────────────────────────────────────────────────────────────────

struct Rpc {
    client: JsonRpcClient,
    rt: tokio::runtime::Runtime,
}

impl Rpc {
    fn new(url: &str) -> Result<Self> {
        Ok(Self { client: JsonRpcClient::connect(url), rt: tokio::runtime::Runtime::new()? })
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
            if let QueryResponseKind::CallResult(result) = response.kind { Ok(result.result) }
            else { anyhow::bail!("unexpected query response") }
        })
    }

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
                        let input_str = if input_raw.is_empty() {
                            String::new()
                        } else {
                            match base64::engine::general_purpose::STANDARD.decode(input_raw) {
                                Ok(decoded) if !decoded.is_empty() => String::from_utf8_lossy(&decoded).to_string(),
                                _ => input_raw.to_string(),
                            }
                        };
                        let limits = req.get("resource_limits");
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
        if let QueryResponseKind::CallResult(result) = response.kind { Ok(result.result) }
        else { anyhow::bail!("unexpected query response") }
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
        Self { inner: std::sync::Mutex::new(NonceCacheInner { nonce: None, block_hash: None }), rpc_url, signer }
    }

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

    fn invalidate(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.nonce = None;
        inner.block_hash = None;
    }

    fn prefill(&self, nonce: u64, block_hash: CryptoHash) {
        let mut inner = self.inner.lock().unwrap();
        if inner.nonce.is_none() {
            inner.nonce = Some(nonce);
            inner.block_hash = Some(block_hash);
        }
    }
}

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
                method_name, args: args_bytes, gas: 100_000_000_000_000, deposit: 0,
            }))],
        };
        let signed_tx = Transaction::V0(transaction).sign(&Signer::InMemory(signer_clone));
        let tx_hash = format!("{:?}", signed_tx.get_hash());
        client.call(methods::send_tx::RpcSendTransactionRequest {
            signed_transaction: signed_tx,
            wait_until: near_primitives::views::TxExecutionStatus::ExecutedOptimistic,
        }).await.map_err(|e| anyhow::anyhow!("send_tx failed: {}", e))?;
        Ok(tx_hash)
    })
}

fn resolve_batch(
    nonce_cache: &NonceCache, signer: &InMemorySigner, contract: &str,
    payloads: Vec<(u64, bool, String, u64, u64)>,
) -> Vec<(u64, Result<String>)> {
    if payloads.is_empty() { return Vec::new(); }

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
            return payloads.into_iter().map(|(id, _, _, _, _)| (id, Err(anyhow::anyhow!("nonce fetch failed: {}", e)))).collect();
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

    match result {
        Ok(tx_hash) => payloads.into_iter().map(|(id, _, _, _, _)| (id, Ok(tx_hash.clone()))).collect(),
        Err(e) => {
            nonce_cache.invalidate();
            payloads.into_iter().map(|(id, _, _, _, _)| (id, Err(anyhow::anyhow!("batch resolve failed: {}", e)))).collect()
        }
    }
}

// ── WASM discovery & execution ──────────────────────────────────────────────

/// Find WASM file in configured search paths.
/// Uses the same search_paths from inlayer.config.
fn find_wasm(config: &DaemonConfig) -> Option<PathBuf> {
    // Check cache first
    if let Some(cached) = WASM_PATH_CACHE.get() {
        if cached.exists() { return Some(cached.clone()); }
    }

    let mut best: Option<(PathBuf, u64)> = None;

    for dir in &config.search_paths {
        let base = PathBuf::from(dir);
        if !base.exists() {
            continue;
        }

        // Search for WASM files in this directory
        if let Ok(entries) = base.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();

                // Direct WASM file
                if path.is_file() && path.extension().map(|e| e == "wasm").unwrap_or(false) {
                    let size = path.metadata().map(|m| m.len()).unwrap_or(u64::MAX);
                    let is_better = best.as_ref().map_or(true, |(_, sz)| size < *sz);
                    if is_better {
                        best = Some((path, size));
                    }
                    continue;
                }

                // Subdirectory - check for target/wasm32-wasip2/release
                if !path.is_dir() { continue; }

                let release = path.join("target").join("wasm32-wasip2").join("release");
                if release.is_dir() {
                    if let Ok(wasm_entries) = release.read_dir() {
                        for wasm_entry in wasm_entries.flatten() {
                            let wasm_path = wasm_entry.path();
                            if wasm_path.is_file() && wasm_path.extension().map(|e| e == "wasm").unwrap_or(false) {
                                let fname = wasm_path.file_name().unwrap_or_default().to_string_lossy();
                                // Skip deps and hidden files
                                if !fname.starts_with('.') && !fname.contains("-deps") {
                                    let size = wasm_path.metadata().map(|m| m.len()).unwrap_or(u64::MAX);
                                    let is_better = best.as_ref().map_or(true, |(_, sz)| size < *sz);
                                    if is_better {
                                        best = Some((wasm_path, size));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some((path, size)) = best {
        eprintln!("Found WASM: {} ({} bytes)", path.display(), size);
        if size > 1_000_000 {
            eprintln!("WASM binary is >1MB. Consider running wasm-opt -Oz.");
        }
        let _ = WASM_PATH_CACHE.set(path.clone());
        Some(path)
    } else {
        eprintln!("No WASM found in search_paths: {:?}", config.search_paths);
        None
    }
}

fn get_wasm_bytes(wasm_path: &Path) -> Result<&'static [u8]> {
    if let Some(cached) = WASM_BYTES_CACHE.get() { return Ok(cached); }
    let bytes = fs::read(wasm_path).with_context(|| format!("reading {}", wasm_path.display()))?;
    let _ = WASM_BYTES_CACHE.set(bytes);
    Ok(WASM_BYTES_CACHE.get().unwrap())
}

struct WasmResult {
    request_id: u64,
    success: bool,
    output: String,
    time_ms: u64,
    instructions: u64,
    error: Option<String>,
    input: String,
}

fn execute_single_wasm(
    wasm_bytes: &[u8],
    request_id: u64,
    input: &str,
    rpc_url: &str,
    env_vars: &HashMap<String, String>,
    req_limits: &RequestInfo,
) -> WasmResult {
    let storage_dir = env::var("STORAGE_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("./storage"));
    fs::create_dir_all(&storage_dir).ok();

    let rpc_cfg = RpcProxyConfig {
        enabled: true,
        rpc_url: Some(rpc_url.to_string()),
        max_calls_per_execution: 100,
        allow_transactions: true,
    };
    let proxy = match RpcProxy::new(rpc_cfg, rpc_url) {
        Ok(p) => p,
        Err(e) => return WasmResult { request_id, success: false, output: String::new(), time_ms: 0, instructions: 0, error: Some(format!("RPC proxy error: {}", e)), input: input.to_string() },
    };

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

    let wasm_checksum = {
        use std::fmt::Write;
        let mut s = String::with_capacity(64);
        for &b in sha2::Sha256::digest(wasm_bytes).iter() { write!(&mut s, "{:02x}", b).unwrap(); }
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
            WasmResult { request_id, success: r.success, output, time_ms: r.execution_time_ms, instructions: r.instructions, error: r.error, input: input.to_string() }
        }
        Err(e) => WasmResult { request_id, success: false, output: String::new(), time_ms: 0, instructions: 0, error: Some(format!("{}", e)), input: input.to_string() },
    }
}

// ── Dashboard HTTP API ──────────────────────────────────────────────────────

use axum::{
    extract::State,
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

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
        axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)).text("ping"),
    )
}

#[derive(Serialize)]
struct StorageEntry { name: String, hex_name: String, size: u64 }

async fn api_storage(State(state): State<Arc<DashboardState>>) -> axum::Json<Vec<StorageEntry>> {
    let dir = &state.storage_dir;
    let mut entries = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let hex_name = e.file_name().to_string_lossy().to_string();
            let decoded = hex::decode(&hex_name).ok().and_then(|b| String::from_utf8(b).ok()).unwrap_or_else(|| hex_name.clone());
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            entries.push(StorageEntry { name: decoded, hex_name, size });
        }
    }
    axum::Json(entries)
}

#[derive(Serialize)]
struct ContractState { pending_request_ids: Vec<u64>, pending_count: usize, contract_id: String }

async fn api_contract(State(state): State<Arc<DashboardState>>) -> axum::Json<ContractState> {
    let rpc_url = state.rpc_url.clone();
    let contract_id = state.contract_id.clone();
    let result = std::thread::spawn(move || -> Result<Vec<u64>> {
        let rpc = Rpc::new(&rpc_url)?;
        get_pending_ids(&rpc, &contract_id)
    }).join().unwrap_or(Ok(vec![]));
    let ids = result.unwrap_or_default();
    axum::Json(ContractState { pending_count: ids.len(), pending_request_ids: ids, contract_id: state.contract_id.clone() })
}

async fn api_call(
    State(state): State<Arc<DashboardState>>,
    axum::extract::Path((_owner, _project)): axum::extract::Path<(String, String)>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    let input_val = match body.get("input") {
        Some(v) => v.clone(),
        None => return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "status": "failed", "error": "Missing 'input' field in request body" })),
        ),
    };
    let input_str = input_val.to_string();
    let search_paths = state.search_paths.clone();
    let rpc_url = state.rpc_url.clone();
    let env_vars = state.env.clone();

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<WasmResult> {
        // Create a temporary config for WASM search
        let temp_config = DaemonConfig {
            search_paths,
            ..Default::default()
        };
        let wasm_path = find_wasm(&temp_config).context("WASM not found. Check search_paths in config.")?;
        let wasm_bytes = get_wasm_bytes(&wasm_path)?;
        let mut env_vars = env_vars;
        env_vars.insert("REQUEST_TYPE".into(), "https".into());
        let limits = RequestInfo { input: String::new(), max_instructions: 10_000_000_000, max_memory_mb: 256, max_execution_seconds: 120 };
        Ok(execute_single_wasm(wasm_bytes, 0, &input_str, &rpc_url, &env_vars, &limits))
    }).await;

    let wasm_result = match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({ "status": "failed", "error": e.to_string() }))),
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({ "status": "failed", "error": format!("WASM execution panicked: {}", e) }))),
    };

    if wasm_result.success {
        let output_val: serde_json::Value = serde_json::from_str(&wasm_result.output).unwrap_or_else(|_| serde_json::json!({ "Text": wasm_result.output }));
        (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({ "status": "completed", "output": output_val, "compute_cost": wasm_result.instructions, "execution_time_ms": wasm_result.time_ms, "instructions": wasm_result.instructions })),
        )
    } else {
        (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({ "status": "failed", "error": wasm_result.error.unwrap_or_else(|| "Unknown error".into()), "output": null, "compute_cost": wasm_result.instructions })),
        )
    }
}

fn spawn_dashboard(addr: &str, state: Arc<DashboardState>) {
    let addr_str = addr.to_string();
    let addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => { eprintln!("Invalid dashboard address '{}': {}", addr, e); return; }
    };
    let state_clone = state.clone();
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => { eprintln!("Dashboard runtime failed: {}", e); return; }
        };
        rt.block_on(async move {
            let app = Router::new()
                .route("/call/:owner/:project", axum::routing::post(api_call))
                .route("/api/status", get(api_status))
                .route("/api/history", get(api_history))
                .route("/api/stream", get(api_stream))
                .route("/api/storage", get(api_storage))
                .route("/api/contract", get(api_contract))
                .layer(CorsLayer::permissive())
                .with_state(state_clone);
            eprintln!("Dashboard: http://{}", addr_str);
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => { eprintln!("Dashboard bind failed: {}", e); return; }
            };
            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("Dashboard server error: {}", e);
            }
        });
    });
}

// ── Daemon management ───────────────────────────────────────────────────────

fn is_running(pid_path: &Path) -> bool {
    if let Ok(pid_str) = fs::read_to_string(pid_path) {
        if let Ok(pid) = pid_str.trim().parse::<u32>() {
            unsafe { return libc::kill(pid as i32, 0) == 0; }
        }
    }
    false
}

fn read_pid(pid_path: &Path) -> Result<u32> {
    let s = fs::read_to_string(pid_path)?;
    Ok(s.trim().parse()?)
}

fn daemonize(_log_path: &Path, pid_path: &Path) -> Result<()> {
    if let Some(parent) = pid_path.parent() { fs::create_dir_all(parent)?; }
    match unsafe { libc::fork() } {
        -1 => anyhow::bail!("fork failed"),
        0 => {
            unsafe { libc::setsid(); libc::close(0); libc::close(1); libc::close(2); };
            let dn = fs::File::open("/dev/null").ok();
            let dn_fd = dn.as_ref().map(|f| f.as_raw_fd()).unwrap_or(-1);
            unsafe { libc::dup(dn_fd); libc::dup(dn_fd); libc::dup(dn_fd); }
            fs::write(pid_path, std::process::id().to_string()).ok();
            Ok(())
        }
        _ => {
            std::thread::sleep(Duration::from_millis(200));
            std::process::exit(0);
        }
    }
}

fn start_daemon_via_launchd() -> Result<()> {
    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());
    if let Some(plist_path) = &plist {
        let status = std::process::Command::new("launchctl")
            .args(["load", &plist_path.display().to_string()])
            .status()?;
        if status.success() { eprintln!("inlayer daemon started via launchd"); }
        else { anyhow::bail!("launchctl load failed"); }
    } else {
        anyhow::bail!("launchd plist not found at ~/Library/LaunchAgents/com.outlayer.layerd.plist");
    }
    Ok(())
}

fn stop_daemon(pid_path: &Path) -> Result<()> {
    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());
    if let Some(plist_path) = &plist {
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &plist_path.display().to_string()])
            .status();
        std::thread::sleep(Duration::from_millis(500));
    }
    if is_running(pid_path) {
        let pid = read_pid(pid_path)?;
        let my_pid = std::process::id();
        if pid == my_pid { eprintln!("Stopped via launchd"); return Ok(()); }
        eprintln!("Stopping inlayer daemon (PID {})...", pid);
        unsafe { libc::kill(pid as i32, libc::SIGTERM); }
        for _ in 0..10 {
            if !is_running(pid_path) { let _ = fs::remove_file(pid_path); eprintln!("Stopped"); return Ok(()); }
            std::thread::sleep(Duration::from_millis(500));
        }
        unsafe { libc::kill(pid as i32, libc::SIGKILL); }
        let _ = fs::remove_file(pid_path);
        eprintln!("Force killed");
    } else {
        let _ = fs::remove_file(pid_path);
        eprintln!("Stopped (was not running)");
    }
    Ok(())
}

fn check_status(pid_path: &Path, log_path: &Path) -> Result<()> {
    if is_running(pid_path) {
        let pid = read_pid(pid_path)?;
        eprintln!("inlayer daemon running (PID {})", pid);
        eprintln!("   Log: {}", log_path.display());
        eprintln!("   PID: {}", pid_path.display());
    } else {
        eprintln!("inlayer daemon not running");
        if pid_path.exists() { eprintln!("   (stale PID file, cleaning up)"); let _ = fs::remove_file(pid_path); }
    }
    Ok(())
}

fn tail_log(log_path: &Path) -> Result<()> {
    if !log_path.exists() { eprintln!("No log file at {}", log_path.display()); return Ok(()); }
    let _ = std::process::Command::new("tail").args(["-20", &log_path.display().to_string()]).status()?;
    Ok(())
}

fn now() -> String {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

// ── Key loading ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KeyFile { private_key: String, account_id: String }

fn load_signer(path: &str) -> Result<InMemorySigner> {
    // Check if key file exists first
    if !std::path::Path::new(path).exists() {
        anyhow::bail!(
            "Key file not found: {}\n\n\
            To fix this:\n\
            1. Create a config file at ~/.inlayer/inlayer.config with:\n\
               contract_id = \"your-contract.testnet\"\n\
               account_id = \"your-account.testnet\"\n\
               key_path = \"~/.near-credentials/testnet/your-account.testnet.json\"\n\
               network = \"testnet\"\n\n\
            2. Or login with NEAR CLI:\n\
               near login --network testnet\n\n\
            3. Then run: inlayer daemon --status",
            path
        );
    }

    let data = std::fs::read_to_string(path).context("reading key file")?;
    let kf: KeyFile = serde_json::from_str(&data).context("parsing key file")?;
    let signer_account_id: near_primitives::types::AccountId = kf.account_id.parse()?;
    let signer_secret_key: near_crypto::SecretKey = kf.private_key.parse()?;
    Ok(InMemorySigner::from_secret_key(signer_account_id, signer_secret_key))
}

// ── Public entry point ──────────────────────────────────────────────────────

/// Daemon configuration fields (merged into inlayer Config).
/// These are daemon-specific settings that complement the base inlayer Config.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub contract_id: String,
    pub account_id: String,
    pub network: String,
    pub key_path: String,
    pub poll_interval_secs: u64,
    pub dashboard_addr: Option<String>,
    /// WASM search directories (shared with inlayer Config)
    pub search_paths: Vec<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        Self {
            contract_id: "outlayer.testnet".to_string(),
            account_id: "your-account.testnet".to_string(),
            network: "testnet".to_string(),
            key_path: format!("{}/.near-credentials/testnet/your-account.testnet.json", home.display()),
            poll_interval_secs: 5,
            dashboard_addr: None,
            search_paths: vec!["./wasi-examples".to_string()],
        }
    }
}

impl DaemonConfig {
    fn validate(&self) -> Result<()> {
        // Check if using default placeholder values
        if self.account_id.contains("your-account") {
            anyhow::bail!(
                "⚠️  Configuration not set up.\n\n\
                Please create ./inlayer.config in your project directory:\n\
                ---\n\
                contract_id = \"outlayer.testnet\"\n\
                account_id = \"your-actual-account.testnet\"\n\
                key_path = \"~/.near-credentials/testnet/your-actual-account.testnet.json\"\n\
                network = \"testnet\"\n\
                search_paths = [\"./wasi-examples\"]\n\
                poll_interval_secs = 5\n\
                ---\n\n\
                💡 Get your account key with: near login --network testnet\n\
                📁 Or create global config at: ~/.inlayer/inlayer.config"
            );
        }

        // Validate key file exists
        if !std::path::Path::new(&self.key_path).exists() {
            anyhow::bail!(
                "⚠️  Key file not found: {}\n\n\
                Run: near login --network {}\n\
                Then update key_path in your ./inlayer.config",
                self.key_path, self.network
            );
        }

        Ok(())
    }

    fn load(config_dir: &Path) -> Self {
        // Priority 1: Current working directory (project-specific config)
        let cwd = env::current_dir().unwrap_or_else(|_| config_dir.to_path_buf());
        for name in &["inlayer.config", "inlayer.config.toml"] {
            let path = cwd.join(name);
            if path.exists() {
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(mut cfg) = toml::from_str::<DaemonConfig>(&s) {
                        cfg.expand_tildes();
                        eprintln!("📁 Using config from: {}", path.display());
                        return cfg;
                    }
                }
            }
        }

        // Priority 2: Config directory parameter (where inlayer was invoked)
        for name in &["inlayer.config", "inlayer.config.toml"] {
            let path = config_dir.join(name);
            if path.exists() {
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(mut cfg) = toml::from_str::<DaemonConfig>(&s) {
                        cfg.expand_tildes();
                        eprintln!("📁 Using config from: {}", path.display());
                        return cfg;
                    }
                }
            }
        }

        // Priority 3: Global config in home directory
        if let Some(home) = dirs::home_dir() {
            let home_config_dir = home.join(".inlayer");
            for name in &["inlayer.config", "inlayer.config.toml"] {
                let path = home_config_dir.join(name);
                if path.exists() {
                    if let Ok(s) = std::fs::read_to_string(&path) {
                        if let Ok(mut cfg) = toml::from_str::<DaemonConfig>(&s) {
                            cfg.expand_tildes();
                            eprintln!("📁 Using global config from: {}", path.display());
                            return cfg;
                        }
                    }
                }
            }
        }

        // No config found - return default
        eprintln!("⚠️  No config file found. Using defaults.");
        eprintln!("   Create ./inlayer.config in your project directory:");
        eprintln!("   ---");
        eprintln!("   contract_id = \"outlayer.testnet\"");
        eprintln!("   account_id = \"your-account.testnet\"");
        eprintln!("   key_path = \"~/.near-credentials/testnet/your-account.testnet.json\"");
        eprintln!("   network = \"testnet\"");
        eprintln!("   search_paths = [\"./wasi-examples\"]");
        eprintln!("   poll_interval_secs = 5");
        eprintln!("   ---");

        let mut cfg = DaemonConfig::default();
        cfg.expand_tildes();
        cfg
    }

    fn expand_tildes(&mut self) {
        if let Some(home) = dirs::home_dir() {
            let home_str = home.display().to_string();
            if self.key_path.starts_with("~/") {
                self.key_path = format!("{}/{}", home_str, &self.key_path[2..]);
            }
            for dir in &mut self.search_paths {
                if dir.starts_with("~/") {
                    *dir = format!("{}/{}", home_str, &dir[2..]);
                }
            }
        }
    }

    fn rpc_url(&self) -> String {
        match self.network.as_str() {
            "mainnet" => "https://rpc.mainnet.near.org".to_string(),
            "testnet" => "https://test.rpc.fastnear.com".to_string(),
            other => format!("https://rpc.{}.near.org", other),
        }
    }

    fn pid_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("layerd.pid")
    }

    fn log_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("layerd.log")
    }
}

/// Main entry point for `inlayer daemon`.
/// Args after "daemon" subcommand are passed here.
pub fn run_daemon(args: &[String], config_dir: &Path) -> Result<()> {
    let daemon_cfg = DaemonConfig::load(config_dir);

    // Determine mode from args
    if args.iter().any(|a| a == "--stop") {
        return stop_daemon(&daemon_cfg.pid_file_path());
    } else if args.iter().any(|a| a == "--start") {
        return start_daemon_via_launchd();
    } else if args.iter().any(|a| a == "--status") {
        return check_status(&daemon_cfg.pid_file_path(), &daemon_cfg.log_file_path());
    } else if args.iter().any(|a| a == "--log") {
        return tail_log(&daemon_cfg.log_file_path());
    }

    let dashboard_addr = parse_dashboard_flag(args).or(daemon_cfg.dashboard_addr.clone());
    let is_daemon = args.iter().any(|a| a == "--daemon");

    // Validate configuration before starting daemon
    daemon_cfg.validate()?;

    if is_daemon {
        let pid_path = daemon_cfg.pid_file_path();
        let log_path = daemon_cfg.log_file_path();
        if is_running(&pid_path) {
            eprintln!("inlayer daemon already running (PID {})", read_pid(&pid_path).unwrap_or_default());
            std::process::exit(1);
        }
        eprintln!("Starting inlayer daemon...");
        eprintln!("   Log: {}", log_path.display());
        eprintln!("   PID: {}", pid_path.display());
        daemonize(&log_path, &pid_path)?;
    } else {
        let pid_path = daemon_cfg.pid_file_path();
        if let Some(parent) = pid_path.parent() { fs::create_dir_all(parent).ok(); }
        fs::write(&pid_path, std::process::id().to_string()).ok();
        eprintln!("⚡ inlayer daemon — OutLayer local worker (direct RPC)");
        eprintln!("   Contract:   {}", daemon_cfg.contract_id);
        eprintln!("   Account:    {}", daemon_cfg.account_id);
        eprintln!("   RPC:        {}", daemon_cfg.rpc_url());
        eprintln!("   Poll:       {}s", daemon_cfg.poll_interval_secs);
        eprintln!("   WASM paths: {:?}", daemon_cfg.search_paths);
    }

    // ── Dashboard setup ────────────────────────────────────────────────
    let (events_tx, _) = broadcast::channel(100);
    let storage_dir = env::var("STORAGE_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("./storage"));

    let rpc_url = daemon_cfg.rpc_url();

    let dashboard_state = Arc::new(DashboardState {
        history: std::sync::Mutex::new(Vec::new()),
        status: std::sync::Mutex::new(DashboardStatusInner {
            start_time: std::time::Instant::now(),
            poll_count: 0,
            last_poll_time: None,
            last_block_height: None,
            contract_id: daemon_cfg.contract_id.clone(),
            account_id: daemon_cfg.account_id.clone(),
            rpc_url: rpc_url.clone(),
            poll_interval_secs: daemon_cfg.poll_interval_secs,
            dashboard_addr: dashboard_addr.clone(),
        }),
        events_tx,
        storage_dir,
        contract_id: daemon_cfg.contract_id.clone(),
        rpc_url: rpc_url.clone(),
        search_paths: daemon_cfg.search_paths.clone(),
        env: HashMap::new(),
    });

    if let Some(ref addr) = dashboard_addr {
        spawn_dashboard(addr, dashboard_state.clone());
    }

    // ── Worker loop ─────────────────────────────────────────────────────
    let signer = load_signer(&daemon_cfg.key_path)?;
    init_compiled_cache(signer_key_bytes(&signer));

    let mut log_file = if is_daemon {
        let log_path = daemon_cfg.log_file_path();
        if let Some(parent) = log_path.parent() { fs::create_dir_all(parent).ok(); }
        fs::OpenOptions::new().create(true).append(true).open(&log_path).ok()
    } else {
        None
    };

    let mut log = |msg: &str| {
        let line = format!("{} {}\n", now(), msg);
        if is_daemon {
            if let Some(ref mut f) = log_file { let _ = f.write_all(line.as_bytes()); }
        } else {
            eprint!("{}", line);
        }
        let _ = dashboard_state.events_tx.send(msg.to_string());
    };

    log(&format!("inlayer daemon started — Contract: {} Account: {} RPC: {}",
        daemon_cfg.contract_id, daemon_cfg.account_id, rpc_url));

    let rpc = Rpc::new(&rpc_url)?;
    let mut processed: HashSet<u64> = HashSet::new();
    let nonce_cache = NonceCache::new(rpc_url.clone(), signer.clone());
    let _pid_path_cleanup = daemon_cfg.pid_file_path();
    // ctrlc_handler(&_pid_path_cleanup); // TODO: add ctrlc handling

    let block_rx = spawn_block_watcher(&daemon_cfg.network, daemon_cfg.poll_interval_secs);
    log("Block watcher started (neardata.xyz event-driven polling)");

    let mut consecutive_errors = 0u32;

    loop {
        let watcher_height = match block_rx.recv_timeout(Duration::from_secs(daemon_cfg.poll_interval_secs * 3)) {
            Ok(h) => h,
            Err(_) => 0,
        };

        if watcher_height > 0 {
            let mut st = dashboard_state.status.lock().unwrap();
            st.last_block_height = Some(watcher_height);
        }

        {
            let mut st = dashboard_state.status.lock().unwrap();
            st.poll_count += 1;
            st.last_poll_time = Some(now());
        }

        match get_pending_ids(&rpc, &daemon_cfg.contract_id) {
            Ok(ids) => {
                consecutive_errors = 0;
                if ids.is_empty() { continue; }

                log(&format!("Pending: {:?}", ids));

                let unprocessed: Vec<u64> = ids.iter().filter(|id| !processed.contains(id)).copied().collect();
                if unprocessed.is_empty() { continue; }

                let infos = rpc.fetch_request_infos(&daemon_cfg.contract_id, &unprocessed);

                let nonce_prefetch = std::thread::spawn({
                    let rpc_url = rpc_url.clone();
                    let signer_clone = signer.clone();
                    move || fetch_nonce_block(&rpc_url, &signer_clone)
                });

                let wasm_path = match find_wasm(&daemon_cfg) {
                    Some(w) => w,
                    None => { log("WASM not found"); continue; }
                };
                log(&format!("   WASM: {}", wasm_path.display()));

                let wasm_bytes = match get_wasm_bytes(&wasm_path) {
                    Ok(b) => b,
                    Err(e) => { log(&format!("   {}", e)); continue; }
                };

                let wasm_results: Vec<WasmResult> = std::thread::scope(|s| {
                    let handles: Vec<_> = infos.into_iter()
                        .filter_map(|(req_id, info_result)| {
                            match info_result {
                                Ok(info) => {
                                    log(&format!("Request #{} — {}", req_id, info.input));
                                    let mut env = HashMap::new();
                                    env.insert("REQUEST_TYPE".into(), "blockchain".into());
                                    let rpc_url = rpc_url.clone();
                                    Some(s.spawn(move || {
                                        execute_single_wasm(wasm_bytes, req_id, &info.input, &rpc_url, &env, &info)
                                    }))
                                }
                                Err(e) => { log(&format!("   Request #{} info failed: {}", req_id, e)); None }
                            }
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

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
                        if hist.len() > 200 { let excess = hist.len().saturating_sub(200); hist.drain(0..excess); }
                    }
                    if result.success {
                        log(&format!("   #{} | {}ms | {} instr", result.request_id, result.time_ms, result.instructions));
                        log(&format!("   {}", result.output));
                    } else {
                        let err = result.error.unwrap_or_default();
                        log(&format!("   #{}: {}", result.request_id, err));
                    }
                    let output = if result.success { result.output.clone() } else { String::new() };
                    (result.request_id, result.success, output, result.time_ms, result.instructions)
                }).collect();

                if let Ok(Ok((nonce, hash))) = nonce_prefetch.join() {
                    nonce_cache.prefill(nonce, hash);
                }

                let resolve_results = resolve_batch(&nonce_cache, &signer, &daemon_cfg.contract_id, resolve_payloads);

                for (req_id, tx_result) in resolve_results {
                    match tx_result {
                        Ok(tx_hash) => log(&format!("   Tx: {}", tx_hash)),
                        Err(e) => log(&format!("   Submit #{} failed: {}", req_id, e)),
                    }
                }

                if processed.len() > 500 {
                    let max_id = processed.iter().max().copied().unwrap_or(0);
                    let min_keep = max_id.saturating_sub(1000);
                    processed.retain(|&id| id > min_keep);
                }
                continue;
            }
            Err(e) => {
                consecutive_errors += 1;
                let backoff = std::cmp::min(daemon_cfg.poll_interval_secs * (1 << std::cmp::min(consecutive_errors, 5)), 300);
                log(&format!("{} (backoff {}s, attempt #{})", e, backoff, consecutive_errors));
                std::thread::sleep(Duration::from_secs(backoff));
                continue;
            }
        }
    }
}

fn parse_dashboard_flag(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == "--dashboard" && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

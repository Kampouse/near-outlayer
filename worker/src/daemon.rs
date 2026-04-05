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

/// Global shared nonce cache - set once after signer loads, used by both /call and daemon loop
/// Global signer for /call tx signing
/// Global contract id

/// Global shared nonce cache — set once after signer loads, used by both /call and daemon loop
static SHARED_NONCE_CACHE: OnceLock<Arc<NonceCache>> = OnceLock::new();

/// Global signer — set once after loading, used by /call for tx signing
static SHARED_SIGNER: OnceLock<InMemorySigner> = OnceLock::new();

/// Global contract id — set once after config loads
static SHARED_CONTRACT_ID: OnceLock<String> = OnceLock::new();
/// Global deposit amount in yoctoNEAR — set once after config loads
static SHARED_DEPOSIT_YOCTO: OnceLock<u128> = OnceLock::new();

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
    signer: std::sync::Mutex<Option<InMemorySigner>>,
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
                        { let s = serde_json::to_string(&req).unwrap_or_default(); eprintln!("   📋 Raw request #{}: {}", req_id, &s[..s.len().min(200)]); }
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

                        // Parse source from contract request
                        let source = parse_source(&req);
                        eprintln!("   📦 Parsed source: {:?}", source);

                        Ok(RequestInfo {
                            input: input_str,
                            max_instructions: limits.and_then(|l| l.get("max_instructions")).and_then(|v| v.as_u64()).unwrap_or(10_000_000_000),
                            max_memory_mb: limits.and_then(|l| l.get("max_memory_mb")).and_then(|v| v.as_u64()).unwrap_or(256) as u32,
                            max_execution_seconds: limits.and_then(|l| l.get("max_execution_seconds")).and_then(|v| v.as_u64()).unwrap_or(60),
                            source,
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

/// Parse the execution source from contract request JSON.
/// Handles WasmUrl, Project, and GitHub sources.
fn parse_source(req: &serde_json::Value) -> ParsedSource {
    // Try resolved_source first (resolved by contract), then execution_source, then code_source
    let source = match req.get("resolved_source") {
        Some(s) => s,
        None => match req.get("execution_source") {
            Some(s) => s,
            None => match req.get("code_source") {
                Some(s) => s,
                None => match req.get("source") {
                    Some(s) => s,
                    None => return ParsedSource::Unknown,
                },
            },
        },
    };

    // WasmUrl: { "WasmUrl": { "url": "...", "hash": "..." } }
    if let Some(wu) = source.get("WasmUrl") {
        let url = wu.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let hash = wu.get("hash").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if !url.is_empty() {
            return ParsedSource::WasmUrl { url, hash };
        }
    }

    // Project: { "Project": { "project_id": "owner/name" } }
    if let Some(proj) = source.get("Project") {
        let project_id = proj.get("project_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if !project_id.is_empty() {
            return ParsedSource::Project { project_id };
        }
    }

    // GitHub: { "GitHub": { "repo": "...", "commit": "..." } }
    // For now we can't compile from GitHub locally, fall through
    if source.get("GitHub").is_some() {
        return ParsedSource::Unknown;
    }

    ParsedSource::Unknown
}

/// Resolve WASM bytes for a given request source.
/// - WasmUrl: download and cache locally
/// - Project: find by project_id in search paths
/// - Unknown: fall back to default WASM discovery
fn resolve_wasm(source: &ParsedSource, config: &DaemonConfig) -> Option<Vec<u8>> {
    match source {
        ParsedSource::WasmUrl { url, hash } => {
            resolve_wasm_from_url(url, hash)
        }
        ParsedSource::Project { project_id } => {
            resolve_wasm_from_project(project_id, config)
        }
        ParsedSource::Unknown => {
            // Fall back to default WASM discovery
            let path = find_wasm(config)?;
            fs::read(&path).ok()
        }
    }
}

/// Find WASM locally by URL filename, then fall back to download.
fn resolve_wasm_from_url(url: &str, _hash: &str) -> Option<Vec<u8>> {
    // Extract filename from URL (e.g. "nostr-identity-zkp-tee-wasip2.wasm")
    let filename = url.rsplit('/').next().unwrap_or("");

    // Search local paths first
    if !filename.is_empty() {
        let home = dirs::home_dir().unwrap_or_default();
        let search_dirs = vec![
            home.join(".openclaw/workspace"),
            home.join(".openclaw/workspace/nostr-identity"),
            PathBuf::from("."),
        ];
        for dir in &search_dirs {
            let candidate = dir.join(filename);
            if candidate.exists() {
                eprintln!("   📦 Local WASM: {}", candidate.display());
                return fs::read(&candidate).ok();
            }
            // Also check for wasip2 variant
            if !filename.contains("wasip2") {
                let p2_name = filename.replace(".wasm", "-wasip2.wasm");
                let candidate2 = dir.join(&p2_name);
                if candidate2.exists() {
                    eprintln!("   📦 Local WASM: {}", candidate2.display());
                    return fs::read(&candidate2).ok();
                }
            }
        }

        // Broader search: find any file matching the filename in search paths
        let workspace = home.join(".openclaw/workspace");
        if let Ok(entries) = walk_wasm_files(&workspace) {
            for path in entries {
                let fname = path.file_name().unwrap_or_default().to_string_lossy();
                if fname == filename || (filename.contains("wasip2") && fname.contains("wasip2") && fname.contains(&filename.replace("-wasip2.wasm", "").replace(".wasm", ""))) {
                    eprintln!("   📦 Local WASM: {}", path.display());
                    return fs::read(&path).ok();
                }
            }
        }
    }

    // Fallback: download (only if no local match)
    eprintln!("   ⬇️ Not found locally, downloading: {}", url);
    let cache_dir = dirs::home_dir().unwrap_or_default().join(".inlayer").join("wasm_cache");
    fs::create_dir_all(&cache_dir).ok();
    let cache_key = {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(url.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let cached_path = cache_dir.join(format!("{}.wasm", cache_key));
    if cached_path.exists() {
        return fs::read(&cached_path).ok();
    }
    let response = reqwest::blocking::ClientBuilder::new()
        .timeout(std::time::Duration::from_secs(60))
        .build().ok()?
        .get(url)
        .send()
        .ok()?;
    if !response.status().is_success() { return None; }
    let bytes = response.bytes().ok()?.to_vec();
    fs::write(&cached_path, &bytes).ok();
    Some(bytes)
}

/// Walk a directory recursively for .wasm files (max depth 3).
fn walk_wasm_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();
    if !dir.exists() { return Ok(results); }
    fn walk(dir: &Path, depth: u32, out: &mut Vec<PathBuf>) {
        if depth > 3 { return; }
        if let Ok(entries) = dir.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().map(|e| e == "wasm").unwrap_or(false) {
                    out.push(path);
                } else if path.is_dir() {
                    walk(&path, depth + 1, out);
                }
            }
        }
    }
    walk(dir, 0, &mut results);
    Ok(results)
}

/// Find WASM by project_id in search paths.
/// Looks for directory matching the project name (e.g. "nostr-identity" from "kampouse.near/nostr-identity")
fn resolve_wasm_from_project(project_id: &str, config: &DaemonConfig) -> Option<Vec<u8>> {
    // Extract project name from "owner/project" or use as-is
    let project_name = project_id.split('/').last().unwrap_or(project_id);

    for dir in &config.search_paths {
        let base = PathBuf::from(dir);
        if !base.exists() { continue; }

        if let Ok(entries) = base.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() { continue; }

                let dirname = path.file_name()?.to_string_lossy();
                // Match by project name (e.g. "nostr-identity" matches "nostr-identity-zkp-tee")
                if !dirname.contains(project_name) { continue; }

                // Check for built WASM
                let release = path.join("target").join("wasm32-wasip2").join("release");
                if release.is_dir() {
                    if let Ok(wasm_entries) = release.read_dir() {
                        for wasm_entry in wasm_entries.flatten() {
                            let wasm_path = wasm_entry.path();
                            if wasm_path.is_file() && wasm_path.extension().map(|e| e == "wasm").unwrap_or(false) {
                                let fname = wasm_path.file_name().unwrap_or_default().to_string_lossy();
                                if !fname.starts_with('.') && !fname.contains("-deps") {
                                    eprintln!("   📦 Project WASM: {}", wasm_path.display());
                                    return fs::read(&wasm_path).ok();
                                }
                            }
                        }
                    }
                }

                // Check for standalone WASM files in the directory
                if let Ok(dir_entries) = path.read_dir() {
                    for entry in dir_entries.flatten() {
                        let p = entry.path();
                        if p.is_file() && p.extension().map(|e| e == "wasm").unwrap_or(false) {
                            let fname = p.file_name().unwrap_or_default().to_string_lossy();
                            if fname.contains("wasip2") || fname.contains("p2") {
                                eprintln!("   📦 Project WASM: {}", p.display());
                                return fs::read(&p).ok();
                            }
                        }
                    }
                }
            }
        }
    }

    eprintln!("   ⚠️ No WASM found for project: {}", project_id);
    None
}

fn get_pending_ids(rpc: &Rpc, contract: &str) -> Result<Vec<u64>> {
    let args = serde_json::to_vec(&serde_json::json!({"from_index": 0, "limit": 10}))?;
    let bytes = rpc.view(contract, "get_pending_request_ids", &args)?;
    if bytes.is_empty() { return Ok(vec![]); }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Parsed execution source from contract request
#[derive(Debug, Clone)]
enum ParsedSource {
    /// Use WASM from a URL (download + cache)
    WasmUrl { url: String, hash: String },
    /// Use registered project WASM (match by project_id in search paths)
    Project { project_id: String },
    /// No source info — fall back to default WASM discovery
    Unknown,
}

struct RequestInfo {
    input: String,
    max_instructions: u64,
    max_memory_mb: u32,
    max_execution_seconds: u64,
    source: ParsedSource,
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
        for attempt in 0..5 {
            let (base_nonce, block_hash) = match nonce_cache.reserve_batch(1) {
                Ok(r) => r,
                Err(e) => return vec![(*req_id, Err(anyhow::anyhow!("nonce fetch failed: {}", e)))],
            };
            let result = resolve_one(&nonce_cache.rpc_url, signer, contract, *req_id, *success, output, *time_ms, *instructions, base_nonce, block_hash);
            match &result {
                Ok(_) => return vec![(*req_id, result)],
                Err(e) if e.to_string().contains("InvalidNonce") => {
                    nonce_cache.invalidate();
                    if attempt < 4 { continue; }
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
    let input_str = match body.get("input") {
        Some(v) => v.as_str().unwrap_or("").to_string(),
        None => return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "status": "failed", "error": "Missing 'input' field" })),
        ),
    };
    let wasm_url_hint = body.get("wasm_url").and_then(|v| v.as_str()).map(|s| s.to_string());
    let rpc_url = state.rpc_url.clone();
    let search_paths = state.search_paths.clone();
    let start = std::time::Instant::now();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value> {
        // 1. Resolve and execute WASM locally (fast)
        let source = if let Some(ref url) = wasm_url_hint {
            ParsedSource::WasmUrl { url: url.clone(), hash: String::new() }
        } else {
            ParsedSource::Unknown
        };
        let temp_config = DaemonConfig { search_paths: search_paths.clone(), ..Default::default() };
        let wasm_bytes = resolve_wasm(&source, &temp_config)
            .or_else(|| find_wasm(&DaemonConfig { search_paths, ..Default::default() }).and_then(|p| fs::read(&p).ok()))
            .ok_or_else(|| anyhow::anyhow!("No WASM found"))?;

        let info = RequestInfo {
            input: input_str.clone(),
            max_instructions: 10_000_000_000,
            max_memory_mb: 256,
            max_execution_seconds: 60,
            source: ParsedSource::Unknown,
        };
        let mut env = HashMap::new();
        env.insert("REQUEST_TYPE".into(), "http".into());

        let wasm_result = execute_single_wasm(&wasm_bytes, 0, &input_str, &rpc_url, &env, &info);
        let elapsed = start.elapsed();

        // 2. Pre-warm nonce upfront, pass to background thread. No contention, no retries.
        let mut tx_hash = "no_signer".to_string();
        if let (Some(signer), Some(contract_id), Some(nonce_cache)) = 
            (SHARED_SIGNER.get(), SHARED_CONTRACT_ID.get(), SHARED_NONCE_CACHE.get()) 
        {
            // Reserve nonce BEFORE spawning thread (atomic, no contention)
            match nonce_cache.reserve_batch(1) {
                Ok((nonce, block_hash)) => {
                    let input_b64 = base64::engine::general_purpose::STANDARD.encode(input_str.as_bytes());
                    let source_json = if let ParsedSource::WasmUrl { ref url, .. } = source {
                        serde_json::json!({"WasmUrl": {"url": url, "hash": "0000000000000000000000000000000000000000000000000000000000000000", "build_target": "wasm32-wasip2"}})
                    } else {
                        serde_json::json!({"WasmUrl": {"url": "local", "hash": "0000000000000000000000000000000000000000000000000000000000000000", "build_target": "wasm32-wasip2"}})
                    };
                    let args = serde_json::json!({
                        "source": source_json,
                        "input_data": input_b64,
                        "resource_limits": {"max_instructions": 10_000_000_000u64, "max_memory_mb": 256u64, "max_execution_seconds": 60u64},
                        "secrets_ref": null, "response_format": null, "payer_account_id": null, "params": null
                    });
                    let receiver_id = match contract_id.parse::<near_primitives::types::AccountId>() {
                        Ok(id) => id,
                        Err(e) => { eprintln!("   /call: contract_id parse error: {}", e); return Ok(serde_json::json!({"status": "error", "error": format!("contract_id parse: {}", e)})); }
                    };
                    // Build + sign tx RIGHT NOW (before spawning) with pre-warmed nonce
                    let tx = TransactionV0 {
                        signer_id: signer.account_id.clone(),
                        public_key: signer.public_key.clone(),
                        nonce,
                        receiver_id,
                        block_hash,
                        actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                            method_name: "request_execution".to_string(),
                            args: serde_json::to_vec(&args).unwrap_or_default(),
                            gas: 300_000_000_000_000,
                            deposit: SHARED_DEPOSIT_YOCTO.get().copied().unwrap_or(7_001_000_000_000_000_000_000u128),
                        }))],
                    };
                    let signed_tx = Transaction::V0(tx).sign(&Signer::InMemory(signer.clone()));
                    tx_hash = format!("{}", signed_tx.get_hash());
                    let rpc_url_bg = rpc_url.clone();
                    // Only send in background (HTTP latency ~300ms) — tx is already signed
                    std::thread::spawn(move || {
                        let client = JsonRpcClient::connect(&rpc_url_bg);
                        let rt = match tokio::runtime::Runtime::new() {
                            Ok(r) => r,
                            Err(_) => return,
                        };
                        let _ = rt.block_on(async {
                            client.call(methods::send_tx::RpcSendTransactionRequest {
                                signed_transaction: signed_tx,
                                wait_until: near_primitives::views::TxExecutionStatus::None,
                            }).await
                        });
                        eprintln!("   /call bg: tx sent (nonce={})", nonce);
                    });
                }
                Err(e) => eprintln!("   /call: nonce pre-warm failed: {}", e),
            }
        }

        Ok(serde_json::json!({
            "status": if wasm_result.success { "completed" } else { "failed" },
            "output": if wasm_result.success { serde_json::from_str::<serde_json::Value>(&wasm_result.output).unwrap_or_else(|_| serde_json::json!(wasm_result.output)) } else { serde_json::Value::Null },
            "error": wasm_result.error,
            "execution_time_ms": elapsed.as_millis() as u64,
            "instructions": wasm_result.instructions,
            "transaction_hash": tx_hash,
        }))
    }).await;

    match result {
        Ok(Ok(response)) => (axum::http::StatusCode::OK, axum::Json(response)),
        Ok(Err(e)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({ "status": "failed", "error": e.to_string() })),
        ),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({ "status": "failed", "error": format!("Execution panicked: {}", e) })),
        ),
    }
}

// Serve WASM files for project registration
async fn api_wasm(
    State(state): State<Arc<DashboardState>>,
    axum::extract::Path((owner, project)): axum::extract::Path<(String, String)>,
) -> (axum::http::StatusCode, axum::body::Body) {
    let search_paths = state.search_paths.clone();

    // Find WASM file for this project
    let result = tokio::task::spawn_blocking(move || -> Option<PathBuf> {
        let temp_config = DaemonConfig {
            search_paths,
            ..Default::default()
        };

        // First find the WASM
        if let Some(wasm_path) = find_wasm(&temp_config) {
            // Check if filename contains project name
            let filename = wasm_path.file_name()?.to_string_lossy();
            if filename.contains(&project) || project.is_empty() {
                return Some(wasm_path);
            }
        }

        // Try more specific search
        for dir in &temp_config.search_paths {
            let base = PathBuf::from(dir);
            if let Ok(entries) = base.read_dir() {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() && path.file_name()?.to_string_lossy().contains(&project) {
                        let release = path.join("target").join("wasm32-wasip2").join("release");
                        if let Ok(wasm_entries) = release.read_dir() {
                            for wasm_entry in wasm_entries.flatten() {
                                let wasm_path = wasm_entry.path();
                                if wasm_path.is_file() && wasm_path.extension().map(|e| e == "wasm").unwrap_or(false) {
                                    return Some(wasm_path);
                                }
                            }
                        }
                    }
                }
            }

        }
        None
    }).await;

    match result {
        Ok(Some(wasm_path)) => match fs::read(&wasm_path) {
            Ok(bytes) => (
                axum::http::StatusCode::OK,
                axum::body::Body::from(bytes),
            ),
            Err(_) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::body::Body::from("Failed to read WASM file"),
            ),
        },
        Ok(None) => (
            axum::http::StatusCode::NOT_FOUND,
            axum::body::Body::from("WASM file not found"),
        ),
        Err(_) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::body::Body::from("Error searching for WASM"),
        ),
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
                .route("/wasm/:owner/:project", get(api_wasm))
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

pub fn load_signer(path: &str) -> Result<InMemorySigner> {
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
    /// Cloudflare tunnel URL (auto-populated when using --tunnel)
    pub tunnel_url: Option<String>,
    /// Deposit for request_execution in yoctoNEAR (default: 7.001 NEAR)
    pub deposit_yocto: u128,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        Self {
            contract_id: "outlayer.kampouse.testnet".to_string(),
            account_id: "your-account.testnet".to_string(),
            network: "testnet".to_string(),
            key_path: format!("{}/.near-credentials/testnet/your-account.testnet.json", home.display()),
            poll_interval_secs: 5,
            dashboard_addr: None,
            search_paths: vec!["./wasi-examples".to_string()],
            tunnel_url: None,
            deposit_yocto: 7_001_000_000_000_000_000_000u128, // 7.001 NEAR
        }
    }
}

impl DaemonConfig {
    fn deposit_yocto(&self) -> u128 {
        self.deposit_yocto
    }
    fn validate(&self) -> Result<()> {
        // Check if using default placeholder values
        if self.account_id.contains("your-account") {
            anyhow::bail!(
                "⚠️  Configuration not set up.\n\n\
                Please create ./inlayer.config in your project directory:\n\
                ---\n\
                contract_id = \"outlayer.kampouse.testnet\"\n\
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

        // Validate key file exists (try both with and without .json extension)
        let key_path = std::path::Path::new(&self.key_path);
        if !key_path.exists() {
            // Try adding .json if not present
            let with_json = format!("{}.json", self.key_path);
            let json_path = std::path::Path::new(&with_json);
            if json_path.exists() {
                // File exists with .json extension - provide helpful message
                anyhow::bail!(
                    "⚠️  Key file not found: {}\n\
                    But found: {}\n\n\
                    Please update your inlayer.config:\n\
                    key_path = \"{}\"",
                    self.key_path, with_json, with_json
                );
            }

            // Try removing .json if present
            let without_json = self.key_path.trim_end_matches(".json");
            let no_json_path = std::path::Path::new(without_json);
            if no_json_path.exists() {
                // File exists without .json extension - provide helpful message
                anyhow::bail!(
                    "⚠️  Key file not found: {}\n\
                    But found: {}\n\n\
                    Please update your inlayer.config:\n\
                    key_path = \"{}\"",
                    self.key_path, without_json, without_json
                );
            }

            // File not found at all
            anyhow::bail!(
                "⚠️  Key file not found: {}\n\n\
                Run: near login --network {}\n\
                Then update key_path in your ./inlayer.config",
                self.key_path, self.network
            );
        }

        Ok(())
    }

    pub fn load(config_dir: &Path) -> Self {
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
        eprintln!("   contract_id = \"outlayer.kampouse.testnet\"");
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

    fn tunnel_pid_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("cloudflared.pid")
    }
}

// ── Cloudflare Tunnel Management ───────────────────────────────────────────

fn spawn_cloudflare_tunnel(port: u16) -> Result<String> {
    eprintln!("🌐 Starting Cloudflare tunnel...");

    let log_path = dirs::home_dir()
        .unwrap_or_default()
        .join(".inlayer")
        .join("cloudflared.log");

    // Create log file
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent).ok();
    }

    // Spawn cloudflared process
    let child = std::process::Command::new("cloudflared")
        .arg("tunnel")
        .arg("--url")
        .arg(format!("http://localhost:{}", port))
        .stdout(std::fs::File::create(&log_path).unwrap())
        .stderr(std::fs::File::create(&log_path).unwrap())
        .spawn()
        .context("failed to spawn cloudflared - is it installed? (brew install cloudflared)")?;

    let pid = child.id();

    // Save PID for cleanup
    let pid_path = dirs::home_dir()
        .unwrap_or_default()
        .join(".inlayer")
        .join("cloudflared.pid");
    fs::write(&pid_path, pid.to_string()).ok();

    eprintln!("   Waiting for tunnel URL...");

    // Wait for tunnel URL to appear in logs (up to 20 seconds)
    for i in 1..=20 {
        std::thread::sleep(Duration::from_secs(1));
        eprint!(".");

        if let Ok(log_content) = fs::read_to_string(&log_path) {
            // Extract URL using regex
            if let Ok(re) = regex::Regex::new(r"https://[a-z0-9-]+\.trycloudflare\.com") {
                if let Some(m) = re.find(&log_content) {
                    let url = m.as_str().to_string();
                    eprintln!();
                    eprintln!("   ✅ Tunnel created!");
                    eprintln!("   📍 URL: {}", url);
                    return Ok(url);
                }
            }
        }
    }

    anyhow::bail!("timeout waiting for tunnel URL")
}

fn stop_cloudflare_tunnel() {
    let pid_path = dirs::home_dir()
        .unwrap_or_default()
        .join(".inlayer")
        .join("cloudflared.pid");

    if let Ok(pid_str) = fs::read_to_string(&pid_path) {
        if let Ok(pid) = pid_str.trim().parse::<u32>() {
            eprintln!("🛑 Stopping Cloudflare tunnel...");
            unsafe { libc::kill(pid as i32, libc::SIGTERM); }
            let _ = std::thread::sleep(Duration::from_millis(500));
            let _ = fs::remove_file(&pid_path);
        }
    }
}

/// Main entry point for `inlayer daemon`.
/// Args after "daemon" subcommand are passed here.
pub fn run_daemon(args: &[String], config_dir: &Path) -> Result<()> {
    let mut daemon_cfg = DaemonConfig::load(config_dir);

    // Determine mode from args
    if args.iter().any(|a| a == "--stop") {
        stop_cloudflare_tunnel(); // Stop tunnel if running
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
    let is_foreground = args.iter().any(|a| a == "--foreground");
    let use_tunnel = args.iter().any(|a| a == "--tunnel");

    // Validate configuration before starting daemon
    daemon_cfg.validate()?;

    // ── Cloudflare Tunnel Setup ───────────────────────────────────────────
    if use_tunnel {
        eprintln!("🌐 Cloudflare tunnel requested...");
        let tunnel_url = spawn_cloudflare_tunnel(8082)?;

        // Save tunnel URL to config
        let config_path = config_dir.join("inlayer.config");
        if config_path.exists() {
            let mut config_str = fs::read_to_string(&config_path)?;
            if !config_str.contains("tunnel_url") {
                config_str.push_str(&format!("\ntunnel_url = \"{}\"\n", tunnel_url));
                fs::write(&config_path, config_str)?;
                eprintln!("💾 Saved tunnel URL to config");
            }
        }

        daemon_cfg.tunnel_url = Some(tunnel_url);
    }

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
        signer: std::sync::Mutex::new(None),
    });

    if let Some(ref addr) = dashboard_addr {
        spawn_dashboard(addr, dashboard_state.clone());
    }

    // ── Worker loop ─────────────────────────────────────────────────────
    let signer = load_signer(&daemon_cfg.key_path)?;
    init_compiled_cache(signer_key_bytes(&signer));

    // Set signer in dashboard state for API calls
    {
        let mut state = dashboard_state.signer.lock().unwrap();
        *state = Some(signer.clone());
    }

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
    let nonce_cache = Arc::new(NonceCache::new(rpc_url.clone(), signer.clone()));
    // Set globals for /call handler access
    SHARED_NONCE_CACHE.set(nonce_cache.clone()).ok();
    SHARED_SIGNER.set(signer.clone()).ok();
    SHARED_CONTRACT_ID.set(daemon_cfg.contract_id.clone()).ok();
    SHARED_DEPOSIT_YOCTO.set(daemon_cfg.deposit_yocto).ok();
    SHARED_DEPOSIT_YOCTO.set(daemon_cfg.deposit_yocto).ok();
    let pid_path = daemon_cfg.pid_file_path();
    // Clean up PID file on Ctrl+C / SIGTERM
    let pid_path_cleanup = pid_path.clone();
    ctrlc::set_handler(move || {
        eprintln!("Received Ctrl+C, shutting down...");
        let _ = std::fs::remove_file(&pid_path_cleanup);
        std::process::exit(0);
    }).ok();

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

                // Resolve WASM per-request based on source (WasmUrl, Project, or default)
                let default_wasm_bytes = find_wasm(&daemon_cfg)
                    .and_then(|p| fs::read(&p).ok());

                let wasm_results: Vec<WasmResult> = std::thread::scope(|s| {
                    let handles: Vec<_> = infos.into_iter()
                        .filter_map(|(req_id, info_result)| {
                            match info_result {
                                Ok(info) => {
                                    log(&format!("Request #{} — {}", req_id, &info.input[..info.input.len().min(80)]));

                                    // Resolve WASM bytes for this specific request
                                    let wasm_bytes = match resolve_wasm(&info.source, &daemon_cfg) {
                                        Some(b) => {
                                            log(&format!("   Source: {:?}", info.source));
                                            b
                                        }
                                        None => match &default_wasm_bytes {
                                            Some(b) => {
                                                log("   Using default WASM (no source match)");
                                                b.clone()
                                            }
                                            None => {
                                                log("   No WASM found for this request, skipping");
                                                return None;
                                            }
                                        }
                                    };

                                    let mut env = HashMap::new();
                                    env.insert("REQUEST_TYPE".into(), "blockchain".into());
                                    let rpc_url = rpc_url.clone();
                                    Some(s.spawn(move || {
                                        execute_single_wasm(&wasm_bytes, req_id, &info.input, &rpc_url, &env, &info)
                                    }))
                                }
                                Err(e) => { log(&format!("   Request #{} info failed: {}", req_id, e)); None }
                            }
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });

                let resolve_payloads: Vec<(u64, bool, String, u64, u64)> = wasm_results.into_iter().map(|result| {
                    // NOTE: Don't insert into processed set yet — only mark after successful resolve
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
                        Ok(tx_hash) => {
                            processed.insert(req_id);
                            log(&format!("   Tx: {}", tx_hash));
                        },
                        Err(e) => {
                            // Don't mark as processed — will retry next poll
                            log(&format!("   Submit #{} failed: {} (will retry)", req_id, e));
                        },
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

    // Cleanup (unreachable in infinite loop, but here for completeness)
    stop_cloudflare_tunnel();

    Ok(())
}

fn parse_dashboard_flag(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == "--dashboard" && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

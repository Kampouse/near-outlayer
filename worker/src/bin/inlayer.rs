use offchainvm_worker::api_client::ExecutionOutput;
use offchainvm_worker::api_client::{ResourceLimits, ResponseFormat};
use offchainvm_worker::config::RpcProxyConfig;
use offchainvm_worker::executor::{ExecutionContext, Executor};
use offchainvm_worker::outlayer_rpc::RpcProxy;
use offchainvm_worker::outlayer_storage::client::StorageConfig;

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use near_crypto::InMemorySigner;
use near_jsonrpc_client::JsonRpcClient;
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::transaction::{Transaction, TransactionV0};
use near_primitives::types::BlockReference;
use near_primitives::views::QueryRequest;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
struct Config {
    rpc: RpcConfig,
    storage: StorageConfigSection,
    runner: RunnerConfig,
    env: HashMap<String, String>,
    search_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct RpcConfig { url: String }

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct StorageConfigSection { mode: String, dir: String }

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct RunnerConfig {
    max_instructions: u64,
    max_memory_mb: u32,
    max_execution_seconds: u64,
    log_level: String,
    default_input: Option<String>,
}

impl Default for RpcConfig {
    fn default() -> Self { Self { url: "https://rpc.testnet.near.org".into() } }
}
impl Default for StorageConfigSection {
    fn default() -> Self { Self { mode: "local".into(), dir: "./storage".into() } }
}
impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_instructions: 10_000_000_000,
            max_memory_mb: 256,
            max_execution_seconds: 60,
            log_level: "info".into(),
            default_input: None,
        }
    }
}

impl Config {
    fn load(dir: &Path) -> Self {
        for name in &["inlayer.config", "inlayer.config.toml"] {
            let path = dir.join(name);
            if let Ok(s) = std::fs::read_to_string(&path) {
                if let Ok(cfg) = toml::from_str(&s) { return cfg; }
            }
        }
        if let Some(home) = dirs::home_dir() {
            for name in &["inlayer.config", "inlayer.config.toml"] {
                let path = home.join(".inlayer").join(name);
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(cfg) = toml::from_str(&s) { return cfg; }
                }
            }
        }
        Config::default()
    }

    fn resolved_search_paths(&self, config_dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if let Some(p) = config_dir.parent() { out.push(p.to_path_buf()); }
        for p in &self.search_paths {
            let exp = if p.starts_with("~/") {
                dirs::home_dir().map(|h| h.join(&p[2..])).unwrap_or_else(|| PathBuf::from(p))
            } else { PathBuf::from(p) };
            if exp.exists() && !out.contains(&exp) { out.push(exp); }
        }
        out
    }
}

fn find_wasm(name: &str, config_dir: &Path, cfg: &Config) -> Result<PathBuf> {
    let p = PathBuf::from(name);
    if p.is_file() { return Ok(p); }
    let with_ext = if name.ends_with(".wasm") { name.to_string() } else { format!("{}.wasm", name) };

    for base in &cfg.resolved_search_paths(config_dir) {
        if let Ok(entries) = base.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let fname = path.file_name().unwrap_or_default().to_string_lossy();
                    if fname == with_ext || fname == name { return Ok(path); }
                    continue;
                }
                if !path.is_dir() { continue; }
                let candidate = path.join(&with_ext);
                if candidate.is_file() { return Ok(candidate); }
                let release = path.join("target").join("wasm32-wasip2").join("release");
                if release.is_dir() {
                    if entry.file_name() == name {
                        if let Ok(rd) = release.read_dir() {
                            for f in rd.flatten() {
                                let fname = f.file_name();
                                let s = fname.to_string_lossy();
                                if s.ends_with(".wasm") && !s.starts_with('.') && !s.contains("-deps") {
                                    return Ok(f.path());
                                }
                            }
                        }
                    }
                    let candidate = release.join(&with_ext);
                    if candidate.is_file() { return Ok(candidate); }
                }
            }
        }
    }
    anyhow::bail!("WASM not found: {}\n  Run `inlayer list` to see available WASMs", name)
}

fn cmd_run(config_dir: &Path, wasm_name: &str, input: &str, rpc_override: Option<&str>) -> Result<()> {
    let cfg = Config::load(config_dir);
    let wasm_path = find_wasm(wasm_name, config_dir, &cfg)?;

    let filter = EnvFilter::try_new(format!(
        "inlayer={},offchainvm_worker={}", cfg.runner.log_level, cfg.runner.log_level
    )).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let rpc_url = rpc_override.map(|s| s.to_string()).unwrap_or(cfg.rpc.url.clone());
    let storage_dir = PathBuf::from(&cfg.storage.dir);
    std::fs::create_dir_all(&storage_dir).ok();

    // Set env vars from config
    for (k, v) in &cfg.env { env::set_var(k, v); }
    env::set_var("STORAGE_DIR", &storage_dir);

    let wasm_bytes = std::fs::read(&wasm_path)
        .with_context(|| format!("reading {}", wasm_path.display()))?;

    eprintln!("🚀 {}", wasm_path.file_name().unwrap_or_default().to_string_lossy());
    eprintln!("   Input: {}", input);
    eprintln!("   RPC: {}", rpc_url);
    eprintln!("   Storage: {} ({})", cfg.storage.mode, cfg.storage.dir);
    eprintln!();

    // Create RPC proxy and storage config in blocking thread
    let rpc_owned = rpc_url.clone();
    let (proxy, storage_config) = std::thread::scope(|s| {
        s.spawn(|| -> Result<(RpcProxy, StorageConfig)> {
            let rpc_cfg = RpcProxyConfig {
                enabled: true,
                rpc_url: Some(rpc_owned.clone()),
                max_calls_per_execution: 100,
                allow_transactions: true,
            };
            let proxy = RpcProxy::new(rpc_cfg, &rpc_owned)?;
            let storage_config = StorageConfig {
                coordinator_url: "http://127.0.0.1:9999".into(),
                coordinator_token: "local".into(),
                keystore_url: "http://127.0.0.1:9998".into(),
                keystore_token: "local".into(),
                project_uuid: "local-test".into(),
                wasm_hash: "00000000".into(),
                account_id: cfg_env("TEE_SIGNER_ID", "test.testnet"),
                tee_mode: "local".into(),
                keystore_tee_session_id: None,
            };
            Ok((proxy, storage_config))
        }).join().unwrap()
    })?;

    // Pass env vars from config to WASM
    let env_vars: HashMap<String, String> = cfg.env.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let handle = rt.handle().clone();

    let exec_ctx = ExecutionContext {
        outlayer_rpc: Some(Arc::new(proxy)),
        storage_config: Some(storage_config),
        runtime_handle: handle,
        compiled_cache: None,
        vrf_config: None,
        wallet_config: None,
    };

    let executor = Executor::new(cfg.runner.max_instructions, true).with_context(exec_ctx);

    let limits = ResourceLimits {
        max_instructions: cfg.runner.max_instructions,
        max_memory_mb: cfg.runner.max_memory_mb,
        max_execution_seconds: cfg.runner.max_execution_seconds,
    };

    let result = rt.block_on(executor.execute(
        &wasm_bytes,
        None,
        input.as_bytes(),
        &limits,
        if env_vars.is_empty() { None } else { Some(env_vars) },
        Some("wasm32-wasip2"),
        &ResponseFormat::Text,
        None,
        None,
        None,
    ))?;

    println!("{}", "=".repeat(60));
    println!("✅ Success: {}", result.success);
    println!("⏱️  Time: {}ms | Instructions: {}", result.execution_time_ms, result.instructions);
    if let Some(output) = &result.output {
        let s = match output {
            ExecutionOutput::Text(t) => t.clone(),
            ExecutionOutput::Json(j) => serde_json::to_string_pretty(j).unwrap_or_default(),
            ExecutionOutput::Bytes(b) => format!("{} bytes", b.len()),
        };
        println!("📤 Output: {}", s);
    }
    if let Some(error) = &result.error { println!("❌ Error: {}", error); }

    // Drop executor first (releases runtime handle), then runtime drops cleanly
    drop(executor);
    drop(rt);
    Ok(())
}

/// Get env var with fallback, checking config env first
fn cfg_env(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn cmd_submit(extra_args: &[String]) -> Result<()> {
    use base64::Engine;

    if extra_args.is_empty() || extra_args[0] == "--help" {
        eprintln!("Usage: inlayer submit <input_json> [--contract <id>] [--account <id>] [--network <net>]");
        eprintln!("       inlayer submit <input_json> --wasm-url <url> --deposit <near>");
        eprintln!();
        eprintln!("Submits an execution request to the OutLayer contract.");
        eprintln!("layerd will pick it up and execute it.");
        std::process::exit(0);
    }

    // Parse args
    let mut input = extra_args[0].clone();
    let mut contract_id = "outlayer.kampouse.testnet".to_string();
    let mut account_id = "kampouse.testnet".to_string();
    let mut network = "testnet".to_string();
    let mut wasm_url = "https://example.com/test.wasm".to_string();
    let mut deposit_str = "0.01".to_string();

    let mut i = 1;
    while i < extra_args.len() {
        match extra_args[i].as_str() {
            "--contract" if i + 1 < extra_args.len() => { contract_id = extra_args[i + 1].clone(); i += 2; }
            "--account" if i + 1 < extra_args.len() => { account_id = extra_args[i + 1].clone(); i += 2; }
            "--network" if i + 1 < extra_args.len() => { network = extra_args[i + 1].clone(); i += 2; }
            "--wasm-url" if i + 1 < extra_args.len() => { wasm_url = extra_args[i + 1].clone(); i += 2; }
            "--deposit" if i + 1 < extra_args.len() => { deposit_str = extra_args[i + 1].clone(); i += 2; }
            other => { input = other.to_string(); i += 1; }
        }
    }

    let rpc_url = match network.as_str() {
        "mainnet" => "https://rpc.mainnet.near.org".to_string(),
        "testnet" => "https://test.rpc.fastnear.com".to_string(),
        other => other.to_string(),
    };

    let deposit: f64 = deposit_str.parse().context("invalid deposit amount")?;
    let deposit_yocto = (deposit * 1e24) as u128;

    let input_b64 = base64::engine::general_purpose::STANDARD.encode(input.as_bytes());

    let args_json = serde_json::json!({
        "source": {
            "WasmUrl": {
                "url": wasm_url,
                "hash": "0000000000000000000000000000000000000000000000000000000000000000",
                "build_target": "wasm32-wasip2"
            }
        },
        "resource_limits": {
            "max_instructions": 500_000_000_000u64,
            "max_memory_mb": 256u32,
            "max_execution_seconds": 60u64
        },
        "input_data": input_b64
    });
    let args_bytes = serde_json::to_vec(&args_json)?;

    eprintln!("📤 Submitting to {}...", contract_id);
    eprintln!("   Input: {}", input);
    eprintln!("   Account: {}", account_id);

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = JsonRpcClient::connect(&rpc_url);
        let signer = find_signer(&account_id, &network)?;

        let query_response = client
            .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKey { account_id: account_id.parse()?, public_key: signer.public_key() },
            })
            .await
            .context("query access key failed")?;

        let nonce = match query_response.kind {
            QueryResponseKind::AccessKey(ak) => ak.nonce,
            _ => anyhow::bail!("unexpected query response"),
        };

        let transaction = TransactionV0 {
            signer_id: account_id.parse()?,
            public_key: signer.public_key.clone(),
            nonce: nonce + 1,
            receiver_id: contract_id.parse()?,
            block_hash: query_response.block_hash,
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "request_execution".into(),
                args: args_bytes,
                gas: 100_000_000_000_000,
                deposit: deposit_yocto,
            }))],
        };

        let signed_tx = Transaction::V0(transaction).sign(&near_crypto::Signer::InMemory(signer));
        let tx_hash = signed_tx.get_hash();

        let result = client
            .call(near_jsonrpc_client::methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: signed_tx,
            })
            .await
            .context("broadcast tx failed")?;

        match &result.status {
            near_primitives::views::FinalExecutionStatus::Failure(e) => {
                anyhow::bail!("Transaction failed: {:?}", e);
            }
            near_primitives::views::FinalExecutionStatus::SuccessValue(_) => {}
            _ => {}
        }

        eprintln!("✅ Submitted! tx: {}", tx_hash);
        eprintln!("   layerd will pick it up automatically.");
        Ok(())
    })
}

/// Find signer key from ~/.near-credentials
fn find_signer(account_id: &str, network: &str) -> Result<InMemorySigner> {
    use near_crypto::SecretKey;
    use near_primitives::types::AccountId;

    let home = dirs::home_dir().context("no home dir")?;
    let key_path = home.join(format!(".near-credentials/{}/{}.json", network, account_id));
    if !key_path.exists() {
        anyhow::bail!("Key not found at {}. Run: near login", key_path.display());
    }
    let data = std::fs::read_to_string(&key_path)
        .with_context(|| format!("reading {}", key_path.display()))?;
    let kf: serde_json::Value = serde_json::from_str(&data)?;
    let private_key = kf["private_key"].as_str().unwrap_or("");
    let secret = if private_key.contains(':') {
        private_key.rsplit(':').next_back().unwrap_or_default().to_string()
    } else {
        private_key.to_string()
    };
    let account_id: AccountId = account_id.parse()?;
    let secret_key: SecretKey = secret.parse()?;
    Ok(InMemorySigner::from_secret_key(account_id, secret_key))
}

fn cmd_status(extra_args: &[String]) -> Result<()> {
    use base64::Engine;

    let mut contract_id = "outlayer.kampouse.testnet".to_string();
    let mut network = "testnet".to_string();

    let mut i = 0;
    while i < extra_args.len() {
        match extra_args[i].as_str() {
            "--contract" if i + 1 < extra_args.len() => { contract_id = extra_args[i + 1].clone(); i += 2; }
            "--network" if i + 1 < extra_args.len() => { network = extra_args[i + 1].clone(); i += 2; }
            _ => { i += 1; }
        }
    }

    let rpc_url = match network.as_str() {
        "mainnet" => "https://rpc.mainnet.near.org".to_string(),
        "testnet" => "https://test.rpc.fastnear.com".to_string(),
        other => format!("https://rpc.{}.near.org", other),
    };

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = JsonRpcClient::connect(&rpc_url);

        let resp = client
            .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::CallFunction {
                    account_id: contract_id.parse()?,
                    method_name: "get_pending_request_ids".into(),
                    args: near_primitives::types::FunctionArgs::from(
                        serde_json::to_vec(&serde_json::json!({"from_index":0,"limit":10}))?
                    ),
                },
            })
            .await
            .context("query pending requests failed")?;

        let ids: Vec<u64> = match resp.kind {
            QueryResponseKind::CallResult(result) => {
                serde_json::from_slice(&result.result).unwrap_or_default()
            }
            _ => anyhow::bail!("unexpected response"),
        };

        if ids.is_empty() {
            eprintln!("✅ No pending requests");
            return Ok(());
        }

        eprintln!("📋 Pending requests: {:?}", ids);

        for id in &ids {
            let args = serde_json::json!({"request_id": id});
            let args_bytes = serde_json::to_vec(&args)?;

            let resp = client
                .call(near_jsonrpc_client::methods::query::RpcQueryRequest {
                    block_reference: BlockReference::latest(),
                    request: QueryRequest::CallFunction {
                        account_id: contract_id.parse()?,
                        method_name: "get_request".into(),
                        args: near_primitives::types::FunctionArgs::from(args_bytes),
                    },
                })
                .await;

            if let Ok(resp) = resp {
                if let QueryResponseKind::CallResult(result) = resp.kind {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&result.result) {
                        let input_b64 = val.get("input_data").and_then(|v| v.as_str()).unwrap_or("");
                        let input_bytes = base64::engine::general_purpose::STANDARD.decode(input_b64).unwrap_or_default();
                        let input_str = String::from_utf8_lossy(&input_bytes);
                        let status = if val.get("response").is_some() { "✅ resolved" } else { "⏳ pending" };
                        eprintln!("   #{} {} input={}", id, status, input_str);
                    }
                }
            }
        }
        Ok(())
    })
}

fn cmd_list(config_dir: &Path) {
    let cfg = Config::load(config_dir);
    let mut all: Vec<PathBuf> = Vec::new();

    for base in &cfg.resolved_search_paths(config_dir) {
        if let Ok(entries) = base.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().map(|e| e == "wasm").unwrap_or(false) {
                    all.push(path); continue;
                }
                if !path.is_dir() { continue; }
                if let Ok(sub) = path.read_dir() {
                    for f in sub.flatten() {
                        if f.path().is_file() && f.path().extension().map(|e| e == "wasm").unwrap_or(false) {
                            all.push(f.path());
                        }
                    }
                }
                let release = path.join("target").join("wasm32-wasip2").join("release");
                if let Ok(rd) = release.read_dir() {
                    for f in rd.flatten() {
                        let fname = f.file_name();
                        let s = fname.to_string_lossy();
                        if s.ends_with(".wasm") && !s.starts_with('.') && !s.contains("/deps/") {
                            all.push(f.path());
                        }
                    }
                }
            }
        }
    }

    if !all.is_empty() {
        all.sort(); all.dedup();
        println!("Available WASMs:");
        for w in &all {
            let size = w.metadata().map(|m| m.len()).unwrap_or(0);
            let rel = cfg.resolved_search_paths(config_dir).iter()
                .filter_map(|b| w.strip_prefix(b).ok()).next().unwrap_or(w);
            println!("  {:60} {:.0} KB", rel.display(), size as f64 / 1024.0);
        }
    } else {
        println!("No WASM files found.\n  Build: cargo build --target wasm32-wasip2 --release\n  Or add search_paths in inlayer.config");
    }
}

fn cmd_config(config_dir: &Path) {
    let cfg = Config::load(config_dir);
    println!("{}", toml::to_string_pretty(&cfg).unwrap_or_else(|e| format!("Error: {}", e)));
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let config_dir = if let Ok(dir) = env::var("INLAYER_DIR") {
        PathBuf::from(dir)
    } else {
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        if cwd.join("inlayer.config").exists() || cwd.join("inlayer.config.toml").exists() { cwd }
        else if let Some(home) = dirs::home_dir() {
            let hc = home.join(".inlayer");
            if hc.join("inlayer.config").exists() || hc.join("inlayer.config.toml").exists() { hc } else { cwd }
        } else { cwd }
    };

    if args.len() < 2 || args[1] == "-h" || args[1] == "--help" || args[1] == "help" {
        eprintln!("inlayer v{} — OutLayer local WASM runner + request submission\n\n\
Usage:\n\
  inlayer run <wasm> <input> [--rpc <url>]    Run WASM locally\n\
  inlayer submit <input> [--wasm-url <url>]   Submit request to contract\n\
  inlayer status [--contract <id>]            Check pending requests\n\
  inlayer list                                List available WASMs\n\
  inlayer config                              Show current config\n\
  inlayer version                             Show version\n\n\
Config: ./inlayer.config or ~/.inlayer/inlayer.config", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }

    if args[1] == "version" || args[1] == "-v" || args[1] == "--version" {
        eprintln!("inlayer {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }

    match args[1].as_str() {
        "run" => {
            if args.len() < 3 {
                eprintln!("Usage: inlayer run <wasm> <input> [--rpc <url>]");
                std::process::exit(1);
            }
            let cfg = Config::load(&config_dir);
            let wasm = &args[2];
            let mut input = cfg.runner.default_input.clone()
                .unwrap_or_else(|| r#"{}"#.to_string());
            let mut rpc_override: Option<String> = None;
            let mut i = 3;
            while i < args.len() {
                if args[i] == "--rpc" && i + 1 < args.len() {
                    rpc_override = Some(args[i + 1].clone()); i += 2;
                } else {
                    input = args[i].clone(); i += 1;
                }
            }
            cmd_run(&config_dir, wasm, &input, rpc_override.as_deref())?;
        }
        "submit" => {
            cmd_submit(&args[2..])?;
        }
        "status" => {
            cmd_status(&args[2..])?;
        }
        "list" | "ls" => cmd_list(&config_dir),
        "config" => cmd_config(&config_dir),
        cmd => { eprintln!("Unknown: {}. Run: inlayer help", cmd); std::process::exit(1); }
    }
    Ok(())
}

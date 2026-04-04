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
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct WorkerConfig {
    contract_id: String,
    account_id: String,
    private_key: String,
    network: String,
    neardata_url: String,
    near_rpc_url: String,
    poll_interval_secs: u64,
    wasm_search_dirs: Vec<String>,
    start_block: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        Self {
            contract_id: "outlayer.kampouse.testnet".into(),
            account_id: "kampouse.testnet".into(),
            private_key: String::new(),
            network: "testnet".into(),
            neardata_url: "https://testnet.neardata.xyz/v0".into(),
            near_rpc_url: "https://rpc.testnet.near.org".into(),
            poll_interval_secs: 2,
            wasm_search_dirs: vec![format!("{}/.openclaw/workspace", home.display())],
            start_block: 0,
        }
    }
}

impl WorkerConfig {
    fn load() -> Self {
        for name in &["layerd.config", "layerd.config.toml"] {
            if let Ok(s) = std::fs::read_to_string(name) {
                if let Ok(cfg) = toml::from_str(&s) { return cfg; }
            }
        }
        if let Some(home) = dirs::home_dir() {
            for name in &["layerd.config", "layerd.config.toml"] {
                let path = home.join(".inlayer").join(name);
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(cfg) = toml::from_str(&s) { return cfg; }
                }
            }
        }
        WorkerConfig::default()
    }
}

// ── Event parsing ───────────────────────────────────────────────────────────

// Events come from neardata as EVENT_JSON logs in receipt_execution_outcomes

// ── Block scanning ──────────────────────────────────────────────────────────

fn get_latest_block(rpc_url: &str) -> Result<u64> {
    let client = reqwest::blocking::Client::new();
    let resp: serde_json::Value = client.post(rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "block",
            "params": {"finality": "final"}
        }))
        .timeout(Duration::from_secs(10))
        .send()?.json()?;
    resp["result"]["header"]["height"]
        .as_u64()
        .context("missing block height in RPC response")
}

fn scan_block(neardata_url: &str, block_height: u64, contract_id: &str) -> Result<Vec<(u64, String, Vec<u8>)>> {
    let client = reqwest::blocking::Client::new();
    let url = format!("{}/block/{}", neardata_url, block_height);
    let resp: serde_json::Value = client.get(&url)
        .timeout(Duration::from_secs(10))
        .send()?.json()?;

    let mut results = Vec::new();

    if let Some(shards) = resp.get("shards").and_then(|s| s.as_array()) {
        for shard in shards {
            // Check receipt execution outcomes
            if let Some(outcomes) = shard.get("receipt_execution_outcomes").and_then(|o| o.as_array()) {
                for outcome in outcomes {
                    let logs = outcome.get("outcome")
                        .and_then(|o| o.get("logs"))
                        .and_then(|l| l.as_array())
                        .cloned()
                        .unwrap_or_default();

                    for log in &logs {
                        let log_str = log.as_str().unwrap_or("");
                        if !log_str.contains(contract_id) && !log_str.contains("execution_requested") {
                            continue;
                        }

                        // Parse EVENT_JSON format
                        if let Some(json_str) = log_str.strip_prefix("EVENT_JSON:") {
                            if let Ok(event) = serde_json::from_str::<serde_json::Value>(json_str) {
                                let event_name = event.get("event").and_then(|e| e.as_str()).unwrap_or("");
                                if event_name == "execution_requested" {
                                    if let Some(data) = event.get("data").and_then(|d| d.as_array()) {
                                        for item in data {
                                            let data_id = item.get("data_id")
                                                .and_then(|v| v.as_str())
                                                .map(|s| hex::decode(s).unwrap_or_default())
                                                .unwrap_or_default();
                                            let request_data_str = item.get("request_data")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("{}");
                                            let rd: serde_json::Value = serde_json::from_str(request_data_str).unwrap_or_default();
                                            let request_id = rd.get("request_id").and_then(|v| v.as_u64()).unwrap_or(0);
                                            let input_data = rd.get("input_data").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                            results.push((request_id, input_data, data_id));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(results)
}

// ── WASM execution ──────────────────────────────────────────────────────────

fn find_wasm(project_name: &str, search_dirs: &[String]) -> Option<PathBuf> {
    for dir in search_dirs {
        let base = PathBuf::from(dir);
        if !base.exists() { continue; }
        // Try direct project name
        for name in &[project_name, "nostr-identity"] {
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

fn execute_wasm(wasm_path: &Path, input: &str, rpc_url: &str) -> Result<(bool, String, u64, u64)> {
    let wasm_bytes = std::fs::read(wasm_path)
        .with_context(|| format!("reading {}", wasm_path.display()))?;

    let storage_dir = PathBuf::from("./storage");
    std::fs::create_dir_all(&storage_dir).ok();
    env::set_var("STORAGE_DIR", &storage_dir);
    env::set_var("TEE_SIGNER_ID", "test.testnet");

    let rpc_owned = rpc_url.to_string();
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
                account_id: "test.testnet".into(),
                tee_mode: "local".into(),
                keystore_tee_session_id: None,
            };
            Ok((proxy, storage_config))
        }).join().unwrap()
    })?;

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

    let executor = Executor::new(10_000_000_000, true).with_context(exec_ctx);

    let limits = ResourceLimits {
        max_instructions: 10_000_000_000,
        max_memory_mb: 256,
        max_execution_seconds: 60,
    };

    let result = rt.block_on(executor.execute(
        &wasm_bytes, None, input.as_bytes(), &limits,
        None, Some("wasm32-wasip2"), &ResponseFormat::Text,
        None, None, None,
    ))?;

    let output = match &result.output {
        Some(ExecutionOutput::Text(t)) => t.clone(),
        Some(ExecutionOutput::Json(j)) => serde_json::to_string(j).unwrap_or_default(),
        Some(ExecutionOutput::Bytes(b)) => format!("{} bytes", b.len()),
        None => String::new(),
    };

    std::mem::forget(rt);
    Ok((result.success, output, result.execution_time_ms, result.instructions))
}

// ── Submit result ───────────────────────────────────────────────────────────

fn submit_result(
    contract_id: &str, account_id: &str, network: &str,
    request_id: u64, success: bool, output: &str,
    time_ms: u64, instructions: u64,
) -> Result<String> {
    let args = serde_json::json!({
        "request_id": request_id,
        "response": {
            "success": success,
            "output": output,
            "error": if success { serde_json::Value::Null } else { serde_json::Value::String("Execution failed".into()) },
            "resources_used": {
                "instructions": instructions,
                "time_ms": time_ms,
            },
            "compilation_note": null,
            "refund_usd": null,
        }
    });

    let args_str = serde_json::to_string(&args)?;

    let output = std::process::Command::new("near")
        .args([
            "call", contract_id, "resolve_execution",
            &args_str,
            "--accountId", account_id,
            "--networkId", network,
            "--gas", "300000000000000",
        ])
        .output()
        .context("failed to run near CLI")?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        anyhow::bail!("near call failed: {}", stderr);
    }

    Ok(stdout)
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let cfg = WorkerConfig::load();

    eprintln!("🔧 layerd");
    eprintln!("   Contract: {}", cfg.contract_id);
    eprintln!("   Account:  {}", cfg.account_id);
    eprintln!("   Network:  {}", cfg.network);
    eprintln!("   Neardata: {}", cfg.neardata_url);
    eprintln!("   RPC:      {}", cfg.near_rpc_url);
    eprintln!();

    // Get current block height
    let mut current_block = if cfg.start_block > 0 {
        cfg.start_block
    } else {
        get_latest_block(&cfg.near_rpc_url)?
    };
    eprintln!("📡 Starting from block {}", current_block);

    loop {
        // Get latest block
        let latest = match get_latest_block(&cfg.neardata_url) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("❌ Failed to get block height: {}", e);
                std::thread::sleep(Duration::from_secs(cfg.poll_interval_secs));
                continue;
            }
        };

        if current_block >= latest {
            std::thread::sleep(Duration::from_secs(cfg.poll_interval_secs));
            continue;
        }

        // Scan new blocks
        for block in (current_block + 1)..=latest {
            match scan_block(&cfg.neardata_url, block, &cfg.contract_id) {
                Ok(events) => {
                    if !events.is_empty() {
                        eprintln!("📦 Block {} — {} events", block, events.len());
                    }

                    for (request_id, input_data, _data_id) in events {
                        eprintln!("\n📋 Request #{}", request_id);
                        eprintln!("   Input: {}", input_data);

                        // Find WASM
                        let wasm = match find_wasm("nostr-identity", &cfg.wasm_search_dirs) {
                            Some(w) => w,
                            None => {
                                eprintln!("   ❌ WASM not found");
                                continue;
                            }
                        };

                        eprintln!("   WASM: {}", wasm.display());
                        eprintln!("   🏃 Running...");

                        // Execute
                        match execute_wasm(&wasm, &input_data, &cfg.near_rpc_url) {
                            Ok((success, output, time_ms, instructions)) => {
                                eprintln!("   ✅ Success: {} | {}ms | {} instructions", success, time_ms, instructions);
                                eprintln!("   📤 Output: {}", output);

                                // Submit result
                                match submit_result(
                                    &cfg.contract_id, &cfg.account_id, &cfg.network,
                                    request_id, success, &output, time_ms, instructions,
                                ) {
                                    Ok(_) => eprintln!("   ✅ Result submitted!"),
                                    Err(e) => eprintln!("   ❌ Submit failed: {}", e),
                                }
                            }
                            Err(e) => {
                                eprintln!("   ❌ Execution failed: {}", e);
                                let _ = submit_result(
                                    &cfg.contract_id, &cfg.account_id, &cfg.network,
                                    request_id, false, "", 0, 0,
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    // Block not indexed yet — normal, just wait
                    if !e.to_string().contains("404") {
                        eprintln!("⚠️  Block {} scan error: {}", block, e);
                    }
                    break;
                }
            }
        }

        current_block = latest;
        std::thread::sleep(Duration::from_secs(cfg.poll_interval_secs));
    }
}

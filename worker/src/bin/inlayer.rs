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
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Deserialize, Serialize)]
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

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc: RpcConfig::default(),
            storage: StorageConfigSection::default(),
            runner: RunnerConfig::default(),
            env: HashMap::new(),
            search_paths: Vec::new(),
        }
    }
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

    std::mem::forget(rt); // Leak runtime to avoid "Cannot drop runtime" panic (CLI tool, negligible)
    Ok(())
}

/// Get env var with fallback, checking config env first
fn cfg_env(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
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
        eprintln!("inlayer — Run WASM components against OutLayer\n\n\
Usage:\n  inlayer run <wasm> <input> [--rpc <url>]\n  inlayer list\n  inlayer config\n\n\
Config: ./inlayer.config or ~/.inlayer/inlayer.config\n\n\
Set runner.default_input in config to provide default input JSON.");
        std::process::exit(0);
    }

    match args[1].as_str() {
        "run" => {
            if args.len() < 3 {
                eprintln!("Usage: inlayer run <wasm> <input> [--rpc <url>]");
                eprintln!("  Or set runner.default_input in inlayer.config");
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
        "list" | "ls" => cmd_list(&config_dir),
        "config" => cmd_config(&config_dir),
        cmd => { eprintln!("Unknown: {}. Run: inlayer help", cmd); std::process::exit(1); }
    }
    Ok(())
}

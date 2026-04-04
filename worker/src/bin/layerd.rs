use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(default)]
struct Config {
    contract_id: String,
    account_id: String,
    network: String,
    poll_interval_secs: u64,
    wasm_search_dirs: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        Self {
            contract_id: "outlayer.kampouse.testnet".into(),
            account_id: "kampouse.testnet".into(),
            network: "testnet".into(),
            poll_interval_secs: 5,
            wasm_search_dirs: vec![format!("{}/.openclaw/workspace", home.display())],
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
}

// ── NEAR view via RPC ───────────────────────────────────────────────────────

fn near_view(contract: &str, method: &str, args: &str, network: &str) -> Result<String> {
    let output = std::process::Command::new("near")
        .args(["view", contract, method, args, "--networkId", network])
        .output()
        .context("near view failed")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        anyhow::bail!("near view: {}", stderr);
    }

    // near-cli-rs output: "Function execution return value: [json]"
    if let Some(idx) = stdout.find("return value") {
        let rest = &stdout[idx + 12..].trim();
        let lines: Vec<&str> = rest.lines().collect();
        if let Some(first) = lines.first() {
            return Ok(first.trim().to_string());
        }
    }

    Ok(stdout.trim().to_string())
}

fn get_pending_ids(contract: &str, network: &str) -> Result<Vec<u64>> {
    let raw = near_view(contract, "get_pending_request_ids", r#"{"from_index":0,"limit":10}"#, network)?;
    // Parse [0, 7] or [7] or []
    let cleaned = raw.trim().trim_start_matches('[').trim_end_matches(']');
    if cleaned.is_empty() {
        return Ok(vec![]);
    }
    let ids: Vec<u64> = cleaned.split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    Ok(ids)
}

fn get_request_input(contract: &str, request_id: u64, network: &str) -> Result<String> {
    let args = format!(r#"{{"request_id":{}}}"#, request_id);
    let raw = near_view(contract, "get_request", &args, network)?;
    // Extract input_data field (base64 encoded)
    let val: serde_json::Value = serde_json::from_str(&raw).context("parse request")?;
    let input_b64 = val.get("input_data").and_then(|v| v.as_str()).unwrap_or("");
    let bytes = base64::decode(input_b64).unwrap_or_default();
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

// ── WASM discovery ──────────────────────────────────────────────────────────

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

// ── Execute via inlayer ─────────────────────────────────────────────────────

fn execute_wasm(wasm_path: &Path, input: &str, network: &str) -> Result<(bool, String, u64, u64)> {
    let rpc = format!("https://rpc.{}.near.org", network);
    let output = std::process::Command::new("inlayer")
        .args(["run", &wasm_path.display().to_string(), input, "--rpc", &rpc])
        .output()
        .context("inlayer run failed")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Parse output
    let success = stdout.contains("✅ Success: true");
    let time_ms = extract_number(&stderr, "Time:")
        .or_else(|| extract_number(&stdout, "Time:"))
        .unwrap_or(0);
    let instructions = extract_number_after(&stdout, "Instructions:")
        .or_else(|| extract_number_after(&stderr, "Instructions:"))
        .unwrap_or(0);

    // Get output payload
    let output_text = stdout.lines()
        .find(|l| l.starts_with("📤 Output:"))
        .map(|l| l.trim_start_matches("📤 Output: ").to_string())
        .unwrap_or_default();

    Ok((success, output_text, time_ms, instructions))
}

fn extract_number(text: &str, prefix: &str) -> Option<u64> {
    text.lines().find(|l| l.contains(prefix))?
        .split_whitespace()
        .find_map(|w| w.parse::<u64>().ok())
}

fn extract_number_after(text: &str, prefix: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.contains(prefix))?;
    let after = line.split(prefix).nth(1)?;
    after.trim().split_whitespace().next()?.parse().ok()
}

// ── Submit result ───────────────────────────────────────────────────────────

fn submit_result(contract: &str, account: &str, network: &str, request_id: u64, success: bool, output: &str, time_ms: u64, instructions: u64) -> Result<()> {
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
    let args_str = serde_json::to_string(&args)?;

    eprintln!("   📤 Submitting...");
    let status = std::process::Command::new("near")
        .args([
            "contract", "call-function", "as-transaction",
            contract, "resolve_execution",
            "json-args", &args_str,
            "prepaid-gas", "100.0 Tgas",
            "attached-deposit", "0 NEAR",
            "sign-as", account,
            "network-config", network,
            "sign-with-keychain", "send",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;

    if status.success() {
        eprintln!("   ✅ Submitted!");
        Ok(())
    } else {
        anyhow::bail!("near call failed");
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cfg = Config::load();

    eprintln!("⚡ layerd — OutLayer local worker");
    eprintln!("   Contract: {}", cfg.contract_id);
    eprintln!("   Account:  {}", cfg.account_id);
    eprintln!("   Poll:     {}s", cfg.poll_interval_secs);
    eprintln!();

    let mut processed: HashSet<u64> = HashSet::new();

    loop {
        match get_pending_ids(&cfg.contract_id, &cfg.network) {
            Ok(ids) => {
                if ids.is_empty() {
                    eprintln!("{} No pending", now());
                } else {
                    eprintln!("{} Pending: {:?}", now(), ids);

                    for req_id in &ids {
                        if processed.contains(req_id) { continue; }
                        eprintln!("\n📋 Request #{}", req_id);

                        // Get input
                        let input = match get_request_input(&cfg.contract_id, *req_id, &cfg.network) {
                            Ok(i) => i,
                            Err(e) => { eprintln!("   ❌ {}", e); continue; }
                        };
                        eprintln!("   Input: {}", input);

                        // Find WASM
                        let wasm = match find_wasm(&cfg.wasm_search_dirs) {
                            Some(w) => w,
                            None => { eprintln!("   ❌ WASM not found"); continue; }
                        };
                        eprintln!("   WASM: {}", wasm.display());

                        // Execute
                        eprintln!("   🏃 Running...");
                        match execute_wasm(&wasm, &input, &cfg.network) {
                            Ok((success, output, time_ms, instructions)) => {
                                eprintln!("   ✅ {} | {}ms | {} instr", success, time_ms, instructions);
                                eprintln!("   📤 {}", output);
                                let _ = submit_result(
                                    &cfg.contract_id, &cfg.account_id, &cfg.network,
                                    *req_id, success, &output, time_ms, instructions,
                                );
                            }
                            Err(e) => {
                                eprintln!("   ❌ {}", e);
                                let _ = submit_result(
                                    &cfg.contract_id, &cfg.account_id, &cfg.network,
                                    *req_id, false, "", 0, 0,
                                );
                            }
                        }
                        processed.insert(*req_id);
                    }
                }
            }
            Err(e) => eprintln!("{} ❌ {}", now(), e),
        }

        std::thread::sleep(Duration::from_secs(cfg.poll_interval_secs));
    }
}

fn now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

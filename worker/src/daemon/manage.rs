//! Daemon configuration and process management.

use std::env;
use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use near_crypto::InMemorySigner;
use serde::{Deserialize, Serialize};

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
    pub fn deposit_yocto(&self) -> u128 {
        self.deposit_yocto
    }

    pub fn validate(&self) -> Result<()> {
        // Check if using default placeholder values
        if self.account_id.contains("your-account") {
            bail!(
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
                bail!(
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
                bail!(
                    "⚠️  Key file not found: {}\n\
                    But found: {}\n\n\
                    Please update your inlayer.config:\n\
                    key_path = \"{}\"",
                    self.key_path, without_json, without_json
                );
            }

            // File not found at all
            bail!(
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

    pub fn rpc_urls(&self) -> Vec<String> {
        match self.network.as_str() {
            "mainnet" => vec![
                "https://rpc.mainnet.near.org".into(),
                "https://near.lava.build".into(),
            ],
            "testnet" => vec![
                "https://rpc.testnet.fastnear.com".into(),
                "https://neart.lava.build".into(),
                "https://near-testnet.gateway.tatum.io".into(),
            ],
            other => vec![format!("https://rpc.{}.near.org", other)],
        }
    }

    pub fn rpc_url(&self) -> String {
        self.rpc_urls().into_iter().next().unwrap()
    }

    pub fn pid_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("layerd.pid")
    }

    pub fn log_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("layerd.log")
    }

    pub fn tunnel_pid_file_path(&self) -> PathBuf {
        let home = dirs::home_dir().unwrap_or_default();
        home.join(".inlayer").join("cloudflared.pid")
    }
}

/// Key file structure for NEAR credentials.
#[derive(Deserialize)]
pub(crate) struct KeyFile {
    pub(crate) private_key: String,
    pub(crate) account_id: String,
}

/// Load a NEAR signer from a key file.
pub fn load_signer(path: &str) -> Result<InMemorySigner> {
    // Check if key file exists first
    if !std::path::Path::new(path).exists() {
        bail!(
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

/// Check if the daemon is currently running.
pub(crate) fn is_running(pid_path: &Path) -> bool {
    if let Ok(pid_str) = fs::read_to_string(pid_path) {
        if let Ok(pid) = pid_str.trim().parse::<u32>() {
            unsafe { return libc::kill(pid as i32, 0) == 0; }
        }
    }
    false
}

/// Read the PID from the PID file.
pub(crate) fn read_pid(pid_path: &Path) -> Result<u32> {
    let s = fs::read_to_string(pid_path)?;
    Ok(s.trim().parse()?)
}

/// Daemonize the current process.
pub(crate) fn daemonize(_log_path: &Path, pid_path: &Path) -> Result<()> {
    if let Some(parent) = pid_path.parent() { fs::create_dir_all(parent)?; }
    match unsafe { libc::fork() } {
        -1 => bail!("fork failed"),
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

/// Start the daemon via launchd (macOS).
pub(crate) fn start_daemon_via_launchd() -> Result<()> {
    let plist = dirs::home_dir()
        .map(|h| h.join("Library/LaunchAgents/com.outlayer.layerd.plist"))
        .filter(|p| p.exists());
    if let Some(plist_path) = &plist {
        let status = std::process::Command::new("launchctl")
            .args(["load", &plist_path.display().to_string()])
            .status()?;
        if status.success() { eprintln!("inlayer daemon started via launchd"); }
        else { bail!("launchctl load failed"); }
    } else {
        bail!("launchd plist not found at ~/Library/LaunchAgents/com.outlayer.layerd.plist");
    }
    Ok(())
}

/// Stop the daemon.
pub(crate) fn stop_daemon(pid_path: &Path) -> Result<()> {
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

/// Check and print the daemon status.
pub(crate) fn check_status(pid_path: &Path, log_path: &Path) -> Result<()> {
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

/// Tail the daemon log file.
pub(crate) fn tail_log(log_path: &Path) -> Result<()> {
    if !log_path.exists() { eprintln!("No log file at {}", log_path.display()); return Ok(()); }
    let _ = std::process::Command::new("tail").args(["-20", &log_path.display().to_string()]).status()?;
    Ok(())
}

/// Parse the --dashboard flag from args.
pub(crate) fn parse_dashboard_flag(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == "--dashboard" && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

/// Get the current time as HH:MM:SS.
pub(crate) fn now() -> String {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

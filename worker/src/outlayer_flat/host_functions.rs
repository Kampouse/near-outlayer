//! Outlayer host function implementations for P2 WASI components.
//!
//! Uses `wasmtime::component::bindgen!` to generate typed bindings from the
//! outlayer:api/host WIT interface. All memory lifting/lowering is handled
//! automatically by wasmtime's canonical ABI.

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;
use wasmtime::component::Linker;

// Generate typed bindings from WIT
wasmtime::component::bindgen!({
    inline: "
        package outlayer:api;

        interface host {
            view: func(contract-id: string, method-name: string, args-json: string) -> result<string, string>;
            call: func(signer-key: string, receiver-id: string, method-name: string, args-json: string, deposit-yocto: string, gas: string) -> result<string, string>;
            transfer: func(signer-key: string, receiver-id: string, amount-yocto: string) -> result<string, string>;
            http-get: func(url: string) -> result<list<u8>, string>;
            http-post: func(url: string, body: list<u8>, content-type: string) -> result<list<u8>, string>;
            storage-set: func(key: string, value: list<u8>) -> result<_, string>;
            storage-get: func(key: string) -> result<option<list<u8>>, string>;
            storage-has: func(key: string) -> result<bool, string>;
            storage-delete: func(key: string) -> result<_, string>;
            storage-increment: func(key: string, delta: s64) -> result<s64, string>;
            storage-decrement: func(key: string, delta: s64) -> result<s64, string>;
            storage-set-if-absent: func(key: string, value: list<u8>) -> result<bool, string>;
            storage-set-if-equals: func(key: string, expected: list<u8>, new-value: list<u8>) -> result<bool, string>;
            storage-list-keys: func(prefix: string) -> result<list<string>, string>;
            storage-clear-all: func() -> result<_, string>;
            storage-set-worker: func(key: string, value: list<u8>) -> result<_, string>;
            storage-get-worker: func(key: string) -> result<option<list<u8>>, string>;
            storage-set-worker-public: func(key: string, value: list<u8>) -> result<_, string>;
            storage-get-worker-from-project: func(key: string, project: string) -> result<option<list<u8>>, string>;
            env-signer: func() -> string;
            env-predecessor: func() -> string;
        }

        world outlayer-world {
            import host;
        }
    ",
});

/// Host state for outlayer functions
pub struct OutlayerHostState {
    storage_dir: PathBuf,
}

impl OutlayerHostState {
    pub fn new() -> Self {
        let storage_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./storage"));
        std::fs::create_dir_all(&storage_dir).ok();
        Self { storage_dir }
    }

    fn safe_path(&self, key: &str) -> PathBuf {
        let hex: String = key.bytes().map(|b| format!("{:02x}", b)).collect();
        self.storage_dir.join(hex)
    }
}

// Implement the generated Host trait — all params are typed Rust values
impl outlayer::api::host::Host for OutlayerHostState {
    fn view(&mut self, contract_id: String, method_name: String, args_json: String) -> Result<String, String> {
        debug!("outlayer::view contract={} method={}", contract_id, method_name);
        // TODO: Wire to real RPC proxy
        Err("RPC not available".into())
    }

    fn call(&mut self, signer_key: String, receiver_id: String, method_name: String, args_json: String, deposit_yocto: String, gas: String) -> Result<String, String> {
        debug!("outlayer::call receiver={} method={}", receiver_id, method_name);
        Err("RPC not available".into())
    }

    fn transfer(&mut self, signer_key: String, receiver_id: String, amount_yocto: String) -> Result<String, String> {
        debug!("outlayer::transfer receiver={} amount={}", receiver_id, amount_yocto);
        Err("RPC not available".into())
    }

    fn http_get(&mut self, url: String) -> Result<Vec<u8>, String> {
        debug!("outlayer::http-get url={}", url);
        std::thread::scope(|s| {
            s.spawn(|| {
                let resp = reqwest::blocking::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build().map_err(|e| e.to_string())?
                    .get(&url)
                    .send().map_err(|e| e.to_string())?
                    .bytes().map_err(|e| e.to_string())?;
                Ok(resp.to_vec())
            }).join().map_err(|_| "thread panicked".to_string())?
        })
    }

    fn http_post(&mut self, url: String, body: Vec<u8>, content_type: String) -> Result<Vec<u8>, String> {
        debug!("outlayer::http-post url={} content_type={}", url, content_type);
        std::thread::scope(|s| {
            s.spawn(move || {
                let resp = reqwest::blocking::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build().map_err(|e| e.to_string())?
                    .post(&url)
                    .header("Content-Type", &content_type)
                    .body(body)
                    .send().map_err(|e| e.to_string())?
                    .bytes().map_err(|e| e.to_string())?;
                Ok(resp.to_vec())
            }).join().map_err(|_| "thread panicked".to_string())?
        })
    }

    fn storage_set(&mut self, key: String, value: Vec<u8>) -> Result<(), String> {
        debug!("outlayer::storage-set key={} len={}", key, value.len());
        let path = self.safe_path(&key);
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
        std::fs::write(&path, &value).map_err(|e| e.to_string())
    }

    fn storage_get(&mut self, key: String) -> Result<Option<Vec<u8>>, String> {
        debug!("outlayer::storage-get key={}", key);
        let path = self.safe_path(&key);
        if path.exists() {
            Ok(Some(std::fs::read(&path).map_err(|e| e.to_string())?))
        } else {
            Ok(None)
        }
    }

    fn storage_has(&mut self, key: String) -> Result<bool, String> {
        Ok(self.safe_path(&key).exists())
    }

    fn storage_delete(&mut self, key: String) -> Result<(), String> {
        let path = self.safe_path(&key);
        if path.exists() { std::fs::remove_file(&path).map_err(|e| e.to_string()) } else { Ok(()) }
    }

    fn storage_increment(&mut self, key: String, delta: i64) -> Result<i64, String> {
        let path = self.safe_path(&key);
        let current = if path.exists() {
            let data = std::fs::read(&path).map_err(|e| e.to_string())?;
            i64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]))
        } else { 0 };
        let new_val = current + delta;
        std::fs::write(&path, new_val.to_le_bytes()).map_err(|e| e.to_string())?;
        Ok(new_val)
    }

    fn storage_decrement(&mut self, key: String, delta: i64) -> Result<i64, String> {
        self.storage_increment(key, -delta)
    }

    fn storage_set_if_absent(&mut self, key: String, value: Vec<u8>) -> Result<bool, String> {
        let path = self.safe_path(&key);
        if path.exists() { return Ok(false); }
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
        std::fs::write(&path, &value).map_err(|e| e.to_string())?;
        Ok(true)
    }

    fn storage_set_if_equals(&mut self, key: String, expected: Vec<u8>, new_value: Vec<u8>) -> Result<bool, String> {
        let path = self.safe_path(&key);
        if path.exists() {
            let current = std::fs::read(&path).map_err(|e| e.to_string())?;
            if current == expected {
                std::fs::write(&path, &new_value).map_err(|e| e.to_string())?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn storage_list_keys(&mut self, prefix: String) -> Result<Vec<String>, String> {
        let dir = &self.storage_dir;
        if !dir.exists() { return Ok(vec![]); }
        let keys: Vec<String> = std::fs::read_dir(dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let hex_str = e.file_name().to_string_lossy().to_string();
                let key: String = (0..hex_str.len())
                    .step_by(2)
                    .filter_map(|i| u8::from_str_radix(&hex_str[i..i+2.min(hex_str.len()-i)], 16).ok())
                    .map(|b| b as char)
                    .collect();
                if key.starts_with(&prefix) { Some(key) } else { None }
            })
            .collect();
        Ok(keys)
    }

    fn storage_clear_all(&mut self) -> Result<(), String> {
        let dir = &self.storage_dir;
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())
    }

    fn storage_set_worker(&mut self, key: String, value: Vec<u8>) -> Result<(), String> {
        // For now, same as storage_set
        self.storage_set(key, value)
    }

    fn storage_get_worker(&mut self, key: String) -> Result<Option<Vec<u8>>, String> {
        self.storage_get(key)
    }

    fn storage_set_worker_public(&mut self, key: String, value: Vec<u8>) -> Result<(), String> {
        self.storage_set(key, value)
    }

    fn storage_get_worker_from_project(&mut self, key: String, project: String) -> Result<Option<Vec<u8>>, String> {
        self.storage_get(key)
    }

    fn env_signer(&mut self) -> String {
        "local".into()
    }

    fn env_predecessor(&mut self) -> String {
        "local".into()
    }
}

/// Add outlayer host functions to a wasmtime component linker
pub fn add_outlayer_to_linker<T: Send + 'static>(
    linker: &mut Linker<T>,
    get_state: impl Fn(&mut T) -> &mut OutlayerHostState + Send + Sync + Copy + 'static,
) -> Result<()> {
    outlayer::api::host::add_to_linker(linker, get_state)
}

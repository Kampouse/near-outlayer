//! Storage host functions for WASM components
//!
//! Implements the `near:storage/api` WIT interface.
//! When keystore is unavailable, falls back to local filesystem.

use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::{debug, warn};
use wasmtime::component::Linker;

use super::client::{StorageClient, StorageConfig};

// Generate bindings from WIT (storage is now separate package near:storage)
wasmtime::component::bindgen!({
    path: "wit",
    world: "near:storage/storage-host",
});

/// Host state for storage functions
pub struct StorageHostState {
    client: StorageClient,
    /// Local filesystem fallback
    local_dir: PathBuf,
    local_cache: Mutex<HashMap<String, Vec<u8>>>,
}

impl StorageHostState {
    /// Create new storage host state from config
    #[allow(dead_code)]
    pub fn new(config: StorageConfig) -> Result<Self> {
        let client = StorageClient::new(config)?;
        let local_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/outlayer-storage"));
        fs::create_dir_all(&local_dir).ok();
        Ok(Self { client, local_dir, local_cache: Mutex::new(HashMap::new()) })
    }

    /// Create new storage host state from existing client
    pub fn from_client(client: StorageClient) -> Self {
        let local_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/outlayer-storage"));
        fs::create_dir_all(&local_dir).ok();
        Self { client, local_dir, local_cache: Mutex::new(HashMap::new()) }
    }

    /// Create local-only storage (no remote coordinator — for inlayer/testing)
    pub fn local_only() -> Result<Self> {
        let local_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./storage"));
        fs::create_dir_all(&local_dir).ok();
        let config = StorageConfig {
            coordinator_url: "http://127.0.0.1:1".into(),
            coordinator_token: "local".into(),
            keystore_url: "http://127.0.0.1:1".into(),
            keystore_token: "local".into(),
            project_uuid: "local-test".into(),
            wasm_hash: "00000000".into(),
            account_id: std::env::var("TEE_SIGNER_ID").unwrap_or_else(|_| "test.testnet".into()),
            tee_mode: "local".into(),
            keystore_tee_session_id: None,
        };
        let client = StorageClient::new(config)?;
        Ok(Self { client, local_dir, local_cache: Mutex::new(HashMap::new()) })
    }

    fn safe_key(&self, key: &str) -> String {
        hex::encode(key.as_bytes())
    }

    fn local_file(&self, key: &str) -> PathBuf {
        self.local_dir.join(self.safe_key(key))
    }

    fn local_set(&self, key: &str, value: &[u8]) -> String {
        self.local_cache.lock().unwrap().insert(key.to_string(), value.to_vec());
        match fs::write(self.local_file(key), value) {
            Ok(()) => String::new(),
            Err(e) => { warn!("local_set failed: {}", e); e.to_string() }
        }
    }

    fn local_get(&self, key: &str) -> (Vec<u8>, String) {
        if let Some(v) = self.local_cache.lock().unwrap().get(key).cloned() {
            return (v, String::new());
        }
        match fs::read(self.local_file(key)) {
            Ok(v) => { self.local_cache.lock().unwrap().insert(key.to_string(), v.clone()); (v, String::new()) }
            Err(_) => (Vec::new(), String::new()),
        }
    }

    fn local_has(&self, key: &str) -> bool {
        self.local_cache.lock().unwrap().contains_key(key) || self.local_file(key).exists()
    }

    fn local_delete(&self, key: &str) -> bool {
        self.local_cache.lock().unwrap().remove(key);
        fs::remove_file(self.local_file(key)).is_ok()
    }
}

// Hex encoding helper
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

impl near::storage::api::Host for StorageHostState {
    fn set(&mut self, key: String, value: Vec<u8>) -> String {
        debug!("storage::set key={}, value_len={}", key, value.len());
        // Try local first (always succeeds), then attempt remote in background
        let local_err = self.local_set(&key, &value);
        if local_err.is_empty() {
            // Try remote in separate thread to avoid tokio panic
            let client = self.client.clone();
            let _ = std::thread::spawn(move || { let _ = client.set(&key, &value); });
        }
        local_err
    }

    fn get(&mut self, key: String) -> (Vec<u8>, String) {
        debug!("storage::get key={}", key);
        // Try local first
        let (data, err) = self.local_get(&key);
        if !data.is_empty() { return (data, err); }
        // Try remote in separate thread to avoid tokio panic
        let client = self.client.clone();
        let key_c = key.clone();
        match std::thread::spawn(move || client.get(&key_c)).join() {
            Ok(Ok(Some(value))) => { self.local_set(&key, &value); (value, String::new()) }
            _ => (Vec::new(), String::new()),
        }
    }

    fn has(&mut self, key: String) -> bool {
        debug!("storage::has key={}", key);
        if self.local_has(&key) { return true; }
        let client = self.client.clone();
        std::thread::spawn(move || client.has(&key).unwrap_or(false))
            .join().unwrap_or(false)
    }

    fn delete(&mut self, key: String) -> bool {
        debug!("storage::delete key={}", key);
        let deleted = self.local_delete(&key);
        let client = self.client.clone();
        std::thread::spawn(move || client.delete(&key).unwrap_or(false))
            .join().unwrap_or(deleted)
    }

    fn list_keys(&mut self, prefix: String) -> (String, String) {
        debug!("storage::list_keys prefix={}", prefix);
        let client = self.client.clone();
        std::thread::spawn(move || client.list_keys(&prefix).unwrap_or_else(|e| { warn!("storage::list_keys failed: {}", e); "[]".into() }))
            .join().map(|v| (v, String::new())).unwrap_or(("[]".into(), String::new()))
    }

    fn set_worker(&mut self, key: String, value: Vec<u8>, is_encrypted: Option<bool>) -> String {
        debug!("storage::set_worker key={}, value_len={}", key, value.len());
        let err = self.local_set(&key, &value);
        let client = self.client.clone();
        let _ = std::thread::spawn(move || { let _ = client.set_worker(&key, &value, is_encrypted.unwrap_or(true)); });
        err
    }

    fn get_worker(&mut self, key: String, project: Option<String>) -> (Vec<u8>, String) {
        debug!("storage::get_worker key={}", key);
        let (data, err) = self.local_get(&key);
        if !data.is_empty() { return (data, err); }
        let client = self.client.clone();
        let key_c = key.clone();
        match std::thread::spawn(move || client.get_worker(&key_c, project.as_deref())).join() {
            Ok(Ok(Some(value))) => { self.local_set(&key, &value); (value, String::new()) }
            _ => (Vec::new(), String::new()),
        }
    }

    fn get_by_version(&mut self, key: String, wasm_hash: String) -> (Vec<u8>, String) {
        debug!("storage::get_by_version key={}", key);
        let (data, err) = self.local_get(&key);
        if !data.is_empty() { return (data, err); }
        let client = self.client.clone();
        match std::thread::spawn(move || client.get_by_version(&key, &wasm_hash)).join() {
            Ok(Ok(Some(value))) => (value, String::new()),
            _ => (Vec::new(), String::new()),
        }
    }

    fn clear_all(&mut self) -> String {
        debug!("storage::clear_all");
        let local_dir = self.local_dir.clone();
        let _ = fs::remove_dir_all(&local_dir); fs::create_dir_all(&local_dir).ok();
        self.local_cache.lock().unwrap().clear();
        let client = self.client.clone();
        std::thread::spawn(move || { let _ = client.clear_all(); }).join().ok();
        String::new()
    }

    fn clear_version(&mut self, wasm_hash: String) -> String {
        debug!("storage::clear_version wasm_hash={}", wasm_hash);
        let client = self.client.clone();
        std::thread::spawn(move || client.clear_version(&wasm_hash).err().map(|e| e.to_string()).unwrap_or_default())
            .join().unwrap_or_default()
    }

    fn set_if_absent(&mut self, key: String, value: Vec<u8>) -> (bool, String) {
        debug!("storage::set_if_absent key={}", key);
        if self.local_has(&key) { return (false, String::new()); }
        let err = self.local_set(&key, &value);
        (err.is_empty(), err)
    }

    fn set_if_equals(&mut self, key: String, expected: Vec<u8>, new_value: Vec<u8>) -> (bool, Vec<u8>, String) {
        debug!("storage::set_if_equals key={}", key);
        let (current, _) = self.local_get(&key);
        if current == expected {
            let err = self.local_set(&key, &new_value);
            (err.is_empty(), Vec::new(), err)
        } else {
            (false, current, String::new())
        }
    }

    fn increment(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::increment key={}, delta={}", key, delta);
        let (current, _) = self.local_get(&key);
        let val = if current.is_empty() { delta } else { i64::from_le_bytes(current[..8].try_into().unwrap_or([0;8])) + delta };
        let err = self.local_set(&key, &val.to_le_bytes());
        (val, err)
    }

    fn decrement(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::decrement key={}, delta={}", key, delta);
        self.increment(key, -delta)
    }
}

/// Add storage host functions to a wasmtime component linker
pub fn add_storage_to_linker<T: Send + 'static>(
    linker: &mut Linker<T>,
    get_state: impl Fn(&mut T) -> &mut StorageHostState + Send + Sync + Copy + 'static,
) -> Result<()> {
    near::storage::api::add_to_linker(linker, get_state)
}

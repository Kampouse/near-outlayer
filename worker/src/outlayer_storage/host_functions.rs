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
        match self.client.set(&key, &value) {
            Ok(()) => String::new(),
            Err(e) => { warn!("storage::set remote failed, using local: {}", e); self.local_set(&key, &value) }
        }
    }

    fn get(&mut self, key: String) -> (Vec<u8>, String) {
        debug!("storage::get key={}", key);
        match self.client.get(&key) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => { warn!("storage::get remote failed, using local: {}", e); self.local_get(&key) }
        }
    }

    fn has(&mut self, key: String) -> bool {
        debug!("storage::has key={}", key);
        self.client.has(&key).unwrap_or_else(|e| { warn!("storage::has remote failed: {}", e); self.local_has(&key) })
    }

    fn delete(&mut self, key: String) -> bool {
        debug!("storage::delete key={}", key);
        self.client.delete(&key).unwrap_or_else(|e| { warn!("storage::delete remote failed: {}", e); self.local_delete(&key) })
    }

    fn list_keys(&mut self, prefix: String) -> (String, String) {
        debug!("storage::list_keys prefix={}", prefix);
        match self.client.list_keys(&prefix) {
            Ok(keys) => (keys, String::new()),
            Err(e) => { warn!("storage::list_keys remote failed: {}", e); (String::from("[]"), String::new()) }
        }
    }

    fn set_worker(&mut self, key: String, value: Vec<u8>, is_encrypted: Option<bool>) -> String {
        let encrypted = is_encrypted.unwrap_or(true);
        debug!("storage::set_worker key={}, value_len={}, is_encrypted={}", key, value.len(), encrypted);
        match self.client.set_worker(&key, &value, encrypted) {
            Ok(()) => String::new(),
            Err(e) => { warn!("storage::set_worker remote failed, using local: {}", e); self.local_set(&key, &value) }
        }
    }

    fn get_worker(&mut self, key: String, project: Option<String>) -> (Vec<u8>, String) {
        debug!("storage::get_worker key={}, project={:?}", key, project);
        match self.client.get_worker(&key, project.as_deref()) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => { warn!("storage::get_worker remote failed, using local: {}", e); self.local_get(&key) }
        }
    }

    fn get_by_version(&mut self, key: String, wasm_hash: String) -> (Vec<u8>, String) {
        debug!("storage::get_by_version key={}, wasm_hash={}", key, wasm_hash);
        match self.client.get_by_version(&key, &wasm_hash) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => { warn!("storage::get_by_version remote failed, using local: {}", e); self.local_get(&key) }
        }
    }

    fn clear_all(&mut self) -> String {
        debug!("storage::clear_all");
        match self.client.clear_all() {
            Ok(()) => String::new(),
            Err(e) => { warn!("storage::clear_all remote failed: {}", e); let _ = fs::remove_dir_all(&self.local_dir); fs::create_dir_all(&self.local_dir).ok(); String::new() }
        }
    }

    fn clear_version(&mut self, wasm_hash: String) -> String {
        debug!("storage::clear_version wasm_hash={}", wasm_hash);
        match self.client.clear_version(&wasm_hash) {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    fn set_if_absent(&mut self, key: String, value: Vec<u8>) -> (bool, String) {
        debug!("storage::set_if_absent key={}, value_len={}", key, value.len());
        match self.client.set_if_absent(&key, &value) {
            Ok(inserted) => (inserted, String::new()),
            Err(e) => {
                warn!("storage::set_if_absent remote failed, using local: {}", e);
                if self.local_has(&key) { (false, String::new()) } else { let err = self.local_set(&key, &value); (err.is_empty(), err) }
            }
        }
    }

    fn set_if_equals(&mut self, key: String, expected: Vec<u8>, new_value: Vec<u8>) -> (bool, Vec<u8>, String) {
        debug!("storage::set_if_equals key={}, expected_len={}, new_len={}", key, expected.len(), new_value.len());
        match self.client.set_if_equals(&key, &expected, &new_value) {
            Ok((success, current)) => (success, current.unwrap_or_default(), String::new()),
            Err(e) => { warn!("storage::set_if_equals remote failed: {}", e); (false, Vec::new(), e.to_string()) }
        }
    }

    fn increment(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::increment key={}, delta={}", key, delta);
        match self.client.increment(&key, delta) {
            Ok(new_value) => (new_value, String::new()),
            Err(e) => {
                warn!("storage::increment remote failed, using local: {}", e);
                let (current, _) = self.local_get(&key);
                let val = if current.is_empty() { delta } else { i64::from_le_bytes(current[..8].try_into().unwrap_or([0;8])) + delta };
                let err = self.local_set(&key, &val.to_le_bytes());
                (val, err)
            }
        }
    }

    fn decrement(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::decrement key={}, delta={}", key, delta);
        match self.client.decrement(&key, delta) {
            Ok(new_value) => (new_value, String::new()),
            Err(e) => {
                warn!("storage::decrement remote failed, using local: {}", e);
                let (current, _) = self.local_get(&key);
                let val = if current.is_empty() { -delta } else { i64::from_le_bytes(current[..8].try_into().unwrap_or([0;8])) - delta };
                let err = self.local_set(&key, &val.to_le_bytes());
                (val, err)
            }
        }
    }
}

/// Add storage host functions to a wasmtime component linker
pub fn add_storage_to_linker<T: Send + 'static>(
    linker: &mut Linker<T>,
    get_state: impl Fn(&mut T) -> &mut StorageHostState + Send + Sync + Copy + 'static,
) -> Result<()> {
    near::storage::api::add_to_linker(linker, get_state)
}

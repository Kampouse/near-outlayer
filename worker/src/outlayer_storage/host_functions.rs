//! Storage host functions for WASM components
//!
//! Implements the `near:storage/api` WIT interface.
//!
//! When running locally (coordinator unavailable), falls back to filesystem storage.

use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;
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
    /// Local filesystem fallback (used when coordinator is unreachable)
    local_dir: PathBuf,
    local_cache: Mutex<HashMap<String, Vec<u8>>>,
}

impl StorageHostState {
    /// Create new storage host state from config
    #[allow(dead_code)]
    pub fn new(config: StorageConfig) -> Result<Self> {
        let local_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/outlayer-storage"));
        fs::create_dir_all(&local_dir).ok();
        let client = StorageClient::new(config)?;
        Ok(Self {
            client,
            local_dir,
            local_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Create new storage host state from existing client
    pub fn from_client(client: StorageClient) -> Self {
        let local_dir = std::env::var("STORAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/outlayer-storage"));
        fs::create_dir_all(&local_dir).ok();
        Self {
            client,
            local_dir,
            local_cache: Mutex::new(HashMap::new()),
        }
    }

    fn safe_key(&self, key: &str) -> PathBuf {
        let hex: String = key.bytes().map(|b| format!("{:02x}", b)).collect();
        self.local_dir.join(hex)
    }

    fn local_set(&self, key: &str, value: &[u8]) -> String {
        let path = self.safe_key(key);
        match fs::write(&path, value) {
            Ok(()) => {
                self.local_cache
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), value.to_vec());
                String::new()
            }
            Err(e) => e.to_string(),
        }
    }

    fn local_get(&self, key: &str) -> (Vec<u8>, String) {
        if let Some(cached) = self.local_cache.lock().unwrap().get(key).cloned() {
            return (cached, String::new());
        }
        let path = self.safe_key(key);
        match fs::read(&path) {
            Ok(data) => {
                self.local_cache
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.clone());
                (data, String::new())
            }
            Err(_) => (Vec::new(), String::new()),
        }
    }

    fn local_has(&self, key: &str) -> bool {
        if self.local_cache.lock().unwrap().contains_key(key) {
            return true;
        }
        self.safe_key(key).exists()
    }

    fn local_delete(&self, key: &str) -> bool {
        self.local_cache.lock().unwrap().remove(key);
        let path = self.safe_key(key);
        path.exists() && fs::remove_file(path).is_ok()
    }
}

impl near::storage::api::Host for StorageHostState {
    fn set(&mut self, key: String, value: Vec<u8>) -> String {
        debug!("storage::set key={}, value_len={}", key, value.len());
        match self.client.set(&key, &value) {
            Ok(()) => String::new(),
            Err(e) => {
                debug!("storage::set remote failed ({}), using local fallback", e);
                self.local_set(&key, &value)
            }
        }
    }

    fn get(&mut self, key: String) -> (Vec<u8>, String) {
        debug!("storage::get key={}", key);
        let result = match self.client.get(&key) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => {
                debug!("storage::get remote failed ({}), using local fallback", e);
                self.local_get(&key)
            }
        };
        result
    }

    fn has(&mut self, key: String) -> bool {
        debug!("storage::has key={}", key);
        match self.client.has(&key) {
            Ok(result) => result,
            Err(e) => {
                debug!("storage::has remote failed ({}), using local fallback", e);
                self.local_has(&key)
            }
        }
    }

    fn delete(&mut self, key: String) -> bool {
        debug!("storage::delete key={}", key);
        match self.client.delete(&key) {
            Ok(result) => result,
            Err(e) => {
                debug!(
                    "storage::delete remote failed ({}), using local fallback",
                    e
                );
                self.local_delete(&key)
            }
        }
    }

    fn list_keys(&mut self, prefix: String) -> (String, String) {
        debug!("storage::list_keys prefix={}", prefix);
        match self.client.list_keys(&prefix) {
            Ok(keys) => (keys, String::new()),
            Err(e) => (String::from("[]"), e.to_string()),
        }
    }

    fn set_worker(&mut self, key: String, value: Vec<u8>, is_encrypted: Option<bool>) -> String {
        let encrypted = is_encrypted.unwrap_or(true);
        debug!(
            "storage::set_worker key={}, value_len={}, is_encrypted={}",
            key,
            value.len(),
            encrypted
        );
        match self.client.set_worker(&key, &value, encrypted) {
            Ok(()) => String::new(),
            Err(e) => {
                debug!(
                    "storage::set_worker remote failed ({}), using local fallback",
                    e
                );
                self.local_set(&key, &value)
            }
        }
    }

    fn get_worker(&mut self, key: String, project: Option<String>) -> (Vec<u8>, String) {
        debug!("storage::get_worker key={}, project={:?}", key, project);
        match self.client.get_worker(&key, project.as_deref()) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => {
                debug!(
                    "storage::get_worker remote failed ({}), using local fallback",
                    e
                );
                self.local_get(&key)
            }
        }
    }

    fn get_by_version(&mut self, key: String, wasm_hash: String) -> (Vec<u8>, String) {
        debug!("storage::get_by_version key={}, wasm_hash={}", key, wasm_hash);
        match self.client.get_by_version(&key, &wasm_hash) {
            Ok(Some(value)) => (value, String::new()),
            Ok(None) => (Vec::new(), String::new()),
            Err(e) => (Vec::new(), e.to_string()),
        }
    }

    fn clear_all(&mut self) -> String {
        debug!("storage::clear_all");
        match self.client.clear_all() {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    fn clear_version(&mut self, wasm_hash: String) -> String {
        debug!("storage::clear_version wasm_hash={}", wasm_hash);
        match self.client.clear_version(&wasm_hash) {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    // ==================== Conditional Write Operations ====================

    fn set_if_absent(&mut self, key: String, value: Vec<u8>) -> (bool, String) {
        debug!("storage::set_if_absent key={}, value_len={}", key, value.len());
        match self.client.set_if_absent(&key, &value) {
            Ok(inserted) => (inserted, String::new()),
            Err(e) => (false, e.to_string()),
        }
    }

    fn set_if_equals(
        &mut self,
        key: String,
        expected: Vec<u8>,
        new_value: Vec<u8>,
    ) -> (bool, Vec<u8>, String) {
        debug!(
            "storage::set_if_equals key={}, expected_len={}, new_len={}",
            key,
            expected.len(),
            new_value.len()
        );
        match self.client.set_if_equals(&key, &expected, &new_value) {
            Ok((success, current)) => (success, current.unwrap_or_default(), String::new()),
            Err(e) => (false, Vec::new(), e.to_string()),
        }
    }

    fn increment(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::increment key={}, delta={}", key, delta);
        match self.client.increment(&key, delta) {
            Ok(new_value) => (new_value, String::new()),
            Err(e) => (0, e.to_string()),
        }
    }

    fn decrement(&mut self, key: String, delta: i64) -> (i64, String) {
        debug!("storage::decrement key={}, delta={}", key, delta);
        match self.client.decrement(&key, delta) {
            Ok(new_value) => (new_value, String::new()),
            Err(e) => (0, e.to_string()),
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

//! Storage host functions for WASM components
//!
//! Implements the `near:storage/api` WIT interface.
//! Local storage uses SQLite for persistence, atomicity, and queries.
//! When running with a coordinator, also syncs to remote storage.

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::{debug, info, warn};
use wasmtime::component::Linker;

use super::client::StorageClient;

// Generate bindings from WIT (storage is now separate package near:storage)
wasmtime::component::bindgen!({
    path: "wit",
    world: "near:storage/storage-host",
});

/// Host state for storage functions
pub struct StorageHostState {
    client: StorageClient,
    /// SQLite database for local persistence (opened once per execution context)
    db: Mutex<rusqlite::Connection>,
    /// In-memory cache for hot keys (avoids SQLite roundtrip within same execution)
    cache: Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl StorageHostState {
    /// Create new storage host state from existing client
    pub fn from_client(client: StorageClient) -> Self {
        let db_path = sqlite_db_path();
        let db = open_db(&db_path);
        Self { client, db: Mutex::new(db), cache: Mutex::new(std::collections::HashMap::new()) }
    }

    /// Create local-only storage (no remote coordinator — for inlayer/testing)
    pub fn local_only() -> Result<Self> {
        let db_path = sqlite_db_path();
        let db = open_db(&db_path);
        let client = StorageClient::new_local()?;
        Ok(Self { client, db: Mutex::new(db), cache: Mutex::new(std::collections::HashMap::new()) })
    }

    fn local_set(&mut self, key: &str, value: &[u8]) -> String {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(key.to_string(), value.to_vec());
        let mut db = self.db.lock().unwrap();
        match db.execute(
            "INSERT INTO kv (key, value, updated_at) VALUES (?1, ?2, unixepoch()) \
             ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = unixepoch()",
            rusqlite::params![key, value],
        ) {
            Ok(_) => String::new(),
            Err(e) => {
                warn!("SQLite set failed for key={}: {}", key, e);
                e.to_string()
            }
        }
    }

    fn local_get(&mut self, key: &str) -> (Vec<u8>, String) {
        // Check in-memory cache first
        if let Some(cached) = self.cache.lock().unwrap().get(key).cloned() {
            return (cached, String::new());
        }
        let mut db = self.db.lock().unwrap();
        match db.query_row("SELECT value FROM kv WHERE key = ?1", rusqlite::params![key], |row| {
            row.get::<_, Vec<u8>>(0)
        }) {
            Ok(data) => {
                self.cache.lock().unwrap().insert(key.to_string(), data.clone());
                (data, String::new())
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => (Vec::new(), String::new()),
            Err(e) => {
                warn!("SQLite get failed for key={}: {}", key, e);
                (Vec::new(), e.to_string())
            }
        }
    }

    fn local_has(&mut self, key: &str) -> bool {
        if self.cache.lock().unwrap().contains_key(key) { return true; }
        let mut db = self.db.lock().unwrap();
        db.query_row("SELECT 1 FROM kv WHERE key = ?1", rusqlite::params![key], |_| Ok(()))
            .is_ok()
    }

    fn local_delete(&mut self, key: &str) -> bool {
        self.cache.lock().unwrap().remove(key);
        let mut db = self.db.lock().unwrap();
        match db.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![key]) {
            Ok(rows) => rows > 0,
            Err(e) => {
                warn!("SQLite delete failed for key={}: {}", key, e);
                false
            }
        }
    }

    fn local_list_keys(&mut self, prefix: &str) -> Vec<String> {
        let mut db = self.db.lock().unwrap();
        let mut stmt = match db.prepare("SELECT key FROM kv WHERE key LIKE ?1 || '%' ORDER BY key") {
            Ok(s) => s,
            Err(e) => {
                warn!("SQLite list_keys failed: {}", e);
                return Vec::new();
            }
        };
        let iter = match stmt.query_map(rusqlite::params![prefix], |row| row.get::<_, String>(0)) {
            Ok(i) => i,
            Err(e) => {
                warn!("SQLite list_keys query failed: {}", e);
                return Vec::new();
            }
        };
        let keys: Vec<String> = iter.filter_map(|r| r.ok()).collect();
        keys
    }

    fn local_clear_all(&mut self) {
        self.cache.lock().unwrap().clear();
        let mut db = self.db.lock().unwrap();
        db.execute("DELETE FROM kv", []).ok();
    }
}

impl near::storage::api::Host for StorageHostState {
    fn set(&mut self, key: String, value: Vec<u8>) -> String {
        debug!("storage::set key={}, value_len={}", key, value.len());
        let local_err = self.local_set(&key, &value);
        if local_err.is_empty() && !self.client.is_local() {
            let client = self.client.clone();
            let _ = std::thread::spawn(move || { let _ = client.set(&key, &value); });
        }
        local_err
    }

    fn get(&mut self, key: String) -> (Vec<u8>, String) {
        debug!("storage::get key={}", key);
        let (data, err) = self.local_get(&key);
        if !data.is_empty() || self.client.is_local() { return (data, err); }
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
        if self.client.is_local() { return false; }
        let client = self.client.clone();
        let key_c = key.clone();
        std::thread::spawn(move || client.has(&key_c).unwrap_or(false))
            .join().unwrap_or(false)
    }

    fn delete(&mut self, key: String) -> bool {
        debug!("storage::delete key={}", key);
        let deleted = self.local_delete(&key);
        if self.client.is_local() { return deleted; }
        let client = self.client.clone();
        let key_c = key.clone();
        std::thread::spawn(move || client.delete(&key_c).unwrap_or(false))
            .join().unwrap_or(deleted)
    }

    fn list_keys(&mut self, prefix: String) -> (String, String) {
        debug!("storage::list_keys prefix={}", prefix);
        if self.client.is_local() {
            let keys: Vec<String> = self.local_list_keys(&prefix)
                .into_iter()
                .map(|k| format!("\"{}\"", k))
                .collect();
            return (format!("[{}]", keys.join(",")), String::new());
        }
        let client = self.client.clone();
        let prefix_c = prefix.clone();
        std::thread::spawn(move || client.list_keys(&prefix_c).unwrap_or_else(|_| "[\"\"".into()))
            .join().map(|v| (v, String::new())).unwrap_or(("[]".into(), String::new()))
    }

    fn set_worker(&mut self, key: String, value: Vec<u8>, is_encrypted: Option<bool>) -> String {
        debug!("storage::set_worker key={}, value_len={}", key, value.len());
        let err = self.local_set(&key, &value);
        if err.is_empty() && !self.client.is_local() {
            let client = self.client.clone();
            let _ = std::thread::spawn(move || { let _ = client.set_worker(&key, &value, is_encrypted.unwrap_or(true)); });
        }
        err
    }

    fn get_worker(&mut self, key: String, project: Option<String>) -> (Vec<u8>, String) {
        debug!("storage::get_worker key={}", key);
        let (data, err) = self.local_get(&key);
        if !data.is_empty() || self.client.is_local() { return (data, err); }
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
        if !data.is_empty() || self.client.is_local() { return (data, err); }
        let client = self.client.clone();
        let key_c = key.clone();
        match std::thread::spawn(move || client.get_by_version(&key_c, &wasm_hash)).join() {
            Ok(Ok(Some(value))) => (value, String::new()),
            _ => (Vec::new(), String::new()),
        }
    }

    fn clear_all(&mut self) -> String {
        debug!("storage::clear_all");
        self.local_clear_all();
        if !self.client.is_local() {
            let client = self.client.clone();
            let _ = std::thread::spawn(move || { let _ = client.clear_all(); });
        }
        String::new()
    }

    fn clear_version(&mut self, wasm_hash: String) -> String {
        debug!("storage::clear_version wasm_hash={}", wasm_hash);
        if self.client.is_local() { return String::new(); }
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

/// Determine the SQLite database path from STORAGE_DIR env var.
/// If STORAGE_DIR ends with .db, use it directly. Otherwise, place storage.db inside it.
fn sqlite_db_path() -> std::path::PathBuf {
    let storage_dir = std::env::var("STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./storage"));
    if storage_dir.extension().map_or(false, |ext| ext == "db") {
        storage_dir
    } else {
        storage_dir.join("storage.db")
    }
}

/// Open (or create) the SQLite database
fn open_db(path: &std::path::Path) -> rusqlite::Connection {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let conn = rusqlite::Connection::open(path)
        .unwrap_or_else(|e| panic!("Failed to open SQLite storage at {}: {}", path.display(), e));
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;")
        .unwrap_or_else(|e| panic!("SQLite PRAGMA failed: {}", e));
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kv (
            key   TEXT PRIMARY KEY,
            value BLOB NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT (unixepoch())
        );"
    ).unwrap_or_else(|e| panic!("Failed to create kv table: {}", e));
    info!("SQLite storage opened: {}", path.display());
    conn
}

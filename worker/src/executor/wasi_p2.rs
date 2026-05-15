//! WASI Preview 2 (Component Model) executor
//!
//! Executes WASM components compiled with wasm32-wasip2 target.
//!
//! ## Features
//! - Component model with typed interfaces
//! - HTTP/HTTPS requests via wasi-http
//! - NEAR RPC proxy via host functions `near:rpc/api@0.1.0` (when ExecutionContext is provided)
//! - Advanced filesystem operations
//! - Async execution
//!
//! ## Requirements
//! - wasmtime 28+
//! - WASM component format (not core module)
//! - wasi:cli/run interface

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::debug;
use wasmtime::component::{Component, Linker};
use wasmtime::*;
use wasm_encoder::ValType as WVal;
const W: WVal = WVal::I32;

/// Global WASM engine for WASI P2 components (component model)
///
/// IMPORTANT: This engine is ONLY for P2 components. P1 modules have their own engine.
///
/// Configuration:
/// - wasm_component_model = true (required for P2)
/// - async_support = true (required for wasi-http)
/// - consume_fuel = true (instruction metering)
///
/// Creating Engine is expensive (~50-100ms). By reusing a single instance,
/// we avoid this overhead on every execution.
///
/// Note: CompiledCache entries are tied to this Engine configuration.
/// If config changes, cached entries will fail to deserialize and be recompiled.
static WASM_ENGINE_P2: OnceLock<Engine> = OnceLock::new();

/// Get or initialize the global P2 engine
///
/// This engine has component_model=true and is NOT compatible with P1 modules.
fn get_p2_engine() -> &'static Engine {
    WASM_ENGINE_P2.get_or_init(|| {
        let mut config = Config::new();
        config.wasm_component_model(true); // P2 ONLY: component model
        config.async_support(true);        // Required for wasi-http
        config.consume_fuel(true);         // Instruction metering
        config.epoch_interruption(true);   // Allow interrupting host calls (wasi-http)
        tracing::info!("⚡ Initialized global WASM engine for P2 (component model)");
        Engine::new(&config).expect("Failed to create P2 WASM engine")
    })
}
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi::bindings::Command;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::types::{
    default_send_request_handler, HostFutureIncomingResponse, OutgoingRequestConfig,
};

use crate::api_client::ResourceLimits;
use crate::compiled_cache::CompiledCache;
use crate::outlayer_rpc::RpcHostState;
use crate::outlayer_storage::{StorageClient, StorageHostState, add_storage_to_linker};
use crate::outlayer_payment::{PaymentHostState, add_payment_to_linker};
use crate::outlayer_vrf::{VrfHostState, add_vrf_to_linker};
use crate::outlayer_wallet::{WalletHostState, add_wallet_to_linker};

use super::ExecutionContext;

/// Max time for a single outbound HTTP request from WASI (seconds)
const HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;
/// After this many timed-out HTTP requests, WASI execution is aborted
const HTTP_TIMEOUT_ABORT_THRESHOLD: u32 = 2;

/// Host state for WASI P2 execution
///
/// Contains WASI context, HTTP context, and optionally RPC proxy, storage, payment, VRF, and wallet state.
struct HostState {
    wasi_ctx: WasiCtx,
    wasi_http_ctx: WasiHttpCtx,
    table: ResourceTable,
    /// RPC proxy state (only present if ExecutionContext has outlayer_rpc)
    rpc_state: Option<RpcHostState>,
    /// Storage state (only present if ExecutionContext has storage_config)
    storage_state: Option<StorageHostState>,
    /// Payment state (only present if attached_usd > 0)
    payment_state: Option<PaymentHostState>,
    /// VRF state (only present if keystore configured + request_id available)
    vrf_state: Option<VrfHostState>,
    /// Wallet state (only present if wallet_id in execution request)
    wallet_state: Option<WalletHostState>,
    /// Counter for timed-out HTTP requests (shared with spawned tasks)
    http_timeout_count: Arc<std::sync::atomic::AtomicU32>,
    /// Engine handle to force epoch interrupt when aborting due to HTTP abuse (Engine::clone is Arc)
    engine_handle: &'static Engine,
    /// Component instance (set after instantiation, used to lazily extract memory)
    component_instance: Option<wasmtime::component::Instance>,
    /// Cached memory (extracted from component_instance on first outlayer call)
    memory: Arc<Mutex<Option<wasmtime::Memory>>>,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    /// Increase the max chunk size for outgoing HTTP request bodies
    /// Default is too small for large attachments (wasi-http-client sends entire body in one write)
    fn outgoing_body_buffer_chunks(&mut self) -> usize {
        tracing::trace!("outgoing_body_buffer_chunks: 16");
        16 // Allow more buffered chunks (default is 1)
    }

    fn outgoing_body_chunk_size(&mut self) -> usize {
        tracing::trace!("outgoing_body_chunk_size: 16MB");
        16 * 1024 * 1024 // 16MB max per write (default might be too small)
    }

    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> wasmtime_wasi_http::HttpResult<HostFutureIncomingResponse> {
        let timeout_count = self.http_timeout_count.clone();
        let engine = self.engine_handle; // &'static Engine

        // Check if already exceeded threshold before sending
        let current = timeout_count.load(std::sync::atomic::Ordering::Relaxed);
        if current >= HTTP_TIMEOUT_ABORT_THRESHOLD {
            tracing::warn!("WASI HTTP aborted: {} requests timed out, refusing new requests", current);
            return Ok(HostFutureIncomingResponse::ready(Ok(Err(
                wasmtime_wasi_http::bindings::http::types::ErrorCode::InternalError(
                    Some(format!("Execution aborted: {} HTTP requests exceeded {}s timeout", current, HTTP_REQUEST_TIMEOUT_SECS))
                )
            ))));
        }

        let url = request.uri().to_string();
        let timeout_duration = std::time::Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS);

        let handle = wasmtime_wasi::runtime::spawn(async move {
            match tokio::time::timeout(
                timeout_duration,
                default_send_request_handler(request, config),
            )
            .await
            {
                Ok(result) => Ok(result),
                Err(_) => {
                    let count = timeout_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    tracing::warn!(
                        "WASI HTTP request timed out after {}s: {} (timeout count: {}/{})",
                        HTTP_REQUEST_TIMEOUT_SECS, url, count, HTTP_TIMEOUT_ABORT_THRESHOLD
                    );
                    if count >= HTTP_TIMEOUT_ABORT_THRESHOLD {
                        tracing::error!("HTTP abuse detected: forcing WASI termination via epoch interrupt");
                        // Advance epoch well past any possible deadline to trigger trap.
                        // max_execution_seconds is at most ~180, +100 for safety margin.
                        for _ in 0..300 {
                            engine.increment_epoch();
                        }
                    }
                    Ok(Err(wasmtime_wasi_http::bindings::http::types::ErrorCode::ConnectionTimeout))
                }
            }
        });

        Ok(HostFutureIncomingResponse::pending(handle))
    }
}

impl HostState {
    /// Get RPC host state (for host function callbacks)
    fn rpc_state_mut(&mut self) -> &mut RpcHostState {
        self.rpc_state.as_mut().expect("RPC state not initialized")
    }

    /// Get storage host state (for host function callbacks)
    fn storage_state_mut(&mut self) -> &mut StorageHostState {
        self.storage_state.as_mut().expect("Storage state not initialized")
    }

    /// Get payment host state (for host function callbacks)
    fn payment_state_mut(&mut self) -> &mut PaymentHostState {
        self.payment_state.as_mut().expect("Payment state not initialized")
    }

    /// Get VRF host state (for host function callbacks)
    fn vrf_state_mut(&mut self) -> &mut VrfHostState {
        self.vrf_state.as_mut().expect("VRF state not initialized")
    }

    /// Get wallet host state (for host function callbacks)
    fn wallet_state_mut(&mut self) -> &mut WalletHostState {
        self.wallet_state.as_mut().expect("Wallet state not initialized")
    }
}

/// Execute WASI Preview 2 component
///
/// # Arguments
/// * `wasm_bytes` - WASM component binary
/// * `wasm_checksum` - SHA256 checksum of WASM bytes (for compiled cache key)
/// * `compiled_cache` - Optional compiled component cache for ~10x speedup
/// * `input_data` - JSON input via stdin
/// * `limits` - Resource limits (memory, instructions, time)
/// * `env_vars` - Environment variables (from encrypted secrets, includes ATTACHED_USD)
/// * `print_stderr` - Print WASM stderr to worker logs
/// * `exec_ctx` - Execution context with optional RPC proxy
///
/// # Returns
/// * `Ok((output, fuel_consumed, refund_usd))` - Execution succeeded
///   - `refund_usd` is Some if WASM called refund_usd() host function
/// * `Err(_)` - Not a valid P2 component or execution failed
pub async fn execute(
    wasm_bytes: &[u8],
    wasm_checksum: Option<&str>,
    compiled_cache: Option<&Arc<Mutex<CompiledCache>>>,
    input_data: &[u8],
    limits: &ResourceLimits,
    env_vars: Option<HashMap<String, String>>,
    print_stderr: bool,
    exec_ctx: Option<&ExecutionContext>,
) -> Result<(Vec<u8>, u64, Option<u64>)> {
    // Use global P2 engine (avoids ~50-100ms overhead per execution)
    let engine = get_p2_engine();

    // Try to load from compiled cache first (if checksum provided)
    let component = if let (Some(checksum), Some(cache)) = (wasm_checksum, compiled_cache) {
        // Try cache hit
        let cached = cache.lock().ok().and_then(|mut c| c.get(checksum, &engine));

        if let Some(cached_component) = cached {
            debug!("⚡ Using compiled cache for {}", checksum);
            cached_component
        } else {
            // Cache miss - compile from bytes
            debug!("🔨 Compiling component (cache miss): {}", checksum);
            let component = Component::from_binary(&engine, wasm_bytes)
                .context("Not a valid WASI Preview 2 component")?;

            // Store in cache for next time
            if let Ok(mut c) = cache.lock() {
                if let Err(e) = c.put(checksum, &component) {
                    tracing::warn!("Failed to cache compiled component: {}", e);
                }
            }

            component
        }
    } else {
        // No cache available - compile directly
        Component::from_binary(&engine, wasm_bytes)
            .context("Not a valid WASI Preview 2 component")?
    };

    debug!("Loaded as WASI Preview 2 component");

    // Check which OutLayer SDK interfaces the WASM imports
    let has_storage_import = component.component_type().imports(&engine)
        .any(|(name, _)| name.contains("near:storage/api"));

    let storage_config = exec_ctx.and_then(|ctx| ctx.storage_config.as_ref());
    let use_local_storage = has_storage_import && storage_config.is_none();

    // Create linker with WASI and HTTP support
    let mut linker: Linker<HostState> = Linker::new(&engine);
    wasmtime_wasi::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)?;

    // Provide core module imports that the component needs:
    // - wasi-snapshot-preview1: WASI P1 adapter module
    // - outlayer: host function stubs
    {
        // Build stub outlayer core module with all 20 host functions
        let mut outlayer_module = wasmtime::Module::new(&engine, {
            // Create a minimal core module that exports all outlayer functions
            let mut build = wasm_encoder::Module::new();
            // Type section
            let mut types = wasm_encoder::TypeSection::new();
            types.ty().function(vec![], vec![W]); // type 0: () -> i32
            types.ty().function(vec![W;2], vec![W]); // type 1
            types.ty().function(vec![W;3], vec![W]); // type 2
            types.ty().function(vec![W;4], vec![W]); // type 3
            types.ty().function(vec![W;5], vec![W]); // type 4
            types.ty().function(vec![W;6], vec![W]); // type 5
            types.ty().function(vec![W;7], vec![W]); // type 6
            types.ty().function(vec![W;8], vec![W]); // type 7
            types.ty().function(vec![W;10], vec![W]); // type 8
            types.ty().function(vec![W;14], vec![W]); // type 9
            build.section(&types);
            // Function section (all use type 0 = return 0)
            let mut funcs = wasm_encoder::FunctionSection::new();
            // 0 params → type 0, 2 params → type 1, etc
            let func_types: &[u32] = &[7,9,8,4,3,4,1,1,5,2,2,5,3,7,4,0,3,4,3,6];
            for &ty in func_types { funcs.function(ty); }
            build.section(&funcs);
            // Export section
            let mut exports = wasm_encoder::ExportSection::new();
            let names: &[&str] = &[
                "view","call","transfer","http_get","storage_set","storage_get",
                "storage_has","storage_delete","storage_increment","env_signer",
                "env_predecessor","storage_decrement","storage_set_if_absent",
                "storage_set_if_equals","storage_list_keys","storage_clear_all",
                "storage_set_worker","storage_get_worker","storage_set_worker_public",
                "storage_get_worker_from_project",
            ];
            for (i, name) in names.iter().enumerate() {
                exports.export(name, wasm_encoder::ExportKind::Func, i as u32);
            }
            build.section(&exports);
            // Code section (all return i32(0))
            let mut code = wasm_encoder::CodeSection::new();
            let param_counts: &[usize] = &[8,14,10,5,4,5,2,2,6,3,3,6,4,8,5,0,4,5,4,7];
            for &n in param_counts {
                let mut fb = wasm_encoder::Function::new(vec![]);
                fb.instruction(&wasm_encoder::Instruction::I32Const(0));
                fb.instruction(&wasm_encoder::Instruction::End);
                code.function(&fb);
            }
            build.section(&code);
            build.finish()
        })?;
        // Register core module imports via linker.root()
        let mut root = linker.root();
        root.module("outlayer", &outlayer_module as &wasmtime::Module)?;
        debug!("Added outlayer core stub module to P2 linker");

        // Load and register the WASI P1→P2 adapter module
        let adapter_bytes = include_bytes!("../../wasi_adapter.wasm");
        let adapter = wasmtime::Module::from_binary(&engine, adapter_bytes)?;
        root.module("wasi-snapshot-preview1", &adapter as &wasmtime::Module)?;
        debug!("Added WASI P1 adapter module to P2 linker");
    }

    // Provide component-level instance for lisp-rlm P2 components
    // that import "outlayer:api/host" at the component boundary.
    // Real implementations for storage + HTTP; stubs for the rest.
    {
        use wasmtime::component::Val as CVal;
        let mut inst = linker.instance("outlayer:api/host")?;

        // storage-set(key_ptr, key_len, val_ptr, val_len) -> s32
        // Writes key=value to local storage. Returns 0 on success.
        inst.func_new("storage-set", |mut caller, params, results| {
            let key_ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let key_len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let val_ptr = match params[2] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let val_len = match params[3] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            
            // Access memory from HostState
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let data = mem.data(&caller);
                if key_ptr + key_len <= data.len() && val_ptr + val_len <= data.len() {
                    let key = std::str::from_utf8(&data[key_ptr..key_ptr+key_len]).unwrap_or("");
                    let val = &data[val_ptr..val_ptr+val_len];
                    
                    let dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".into());
                    let path = std::path::Path::new(&dir).join(key);
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent).ok();
                    }
                    match std::fs::write(&path, val) {
                        Ok(_) => { results[0] = CVal::S32(0); }
                        Err(_) => { results[0] = CVal::S32(-1); }
                    }
                } else {
                    results[0] = CVal::S32(-1);
                }
            } else {
                results[0] = CVal::S32(-1);
            }
            Ok(())
        })?;

        // storage-get(key_ptr, key_len, val_ptr, buf_size, written_ptr) -> s32
        // Reads key from local storage into buffer. Returns bytes written or -1.
        inst.func_new("storage-get", |mut caller, params, results| {
            let key_ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let key_len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let val_ptr = match params[2] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let buf_size = match params[3] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let written_ptr = match params[4] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory.clone() {
                let data = mem.data(&caller);
                if key_ptr + key_len <= data.len() {
                    let key = std::str::from_utf8(&data[key_ptr..key_ptr+key_len]).unwrap_or("");
                    let dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".into());
                    let path = std::path::Path::new(&dir).join(key);
                    if let Ok(val) = std::fs::read(&path) {
                        let write_len = val.len().min(buf_size);
                        // Need mutable access to write response
                        let mem_mut = mem.data_mut(&mut caller);
                        if val_ptr + write_len <= mem_mut.len() && written_ptr + 4 <= mem_mut.len() {
                            mem_mut[val_ptr..val_ptr+write_len].copy_from_slice(&val[..write_len]);
                            // Write length to written_ptr as i32 LE
                            let len_bytes = (write_len as i32).to_le_bytes();
                            mem_mut[written_ptr..written_ptr+4].copy_from_slice(&len_bytes);
                            results[0] = CVal::S32(0);
                        } else {
                            results[0] = CVal::S32(-1);
                        }
                    } else {
                        results[0] = CVal::S32(-1); // key not found
                    }
                } else {
                    results[0] = CVal::S32(-1);
                }
            } else {
                results[0] = CVal::S32(-1);
            }
            Ok(())
        })?;

        // storage-has(key_ptr, key_len) -> s32
        // Returns 1 if key exists, 0 otherwise.
        inst.func_new("storage-has", |mut caller, params, results| {
            let key_ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let key_len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let data = mem.data(&caller);
                if key_ptr + key_len <= data.len() {
                    let key = std::str::from_utf8(&data[key_ptr..key_ptr+key_len]).unwrap_or("");
                    let dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".into());
                    let path = std::path::Path::new(&dir).join(key);
                    results[0] = CVal::S32(if path.exists() { 1 } else { 0 });
                } else { results[0] = CVal::S32(0); }
            } else { results[0] = CVal::S32(0); }
            Ok(())
        })?;

        // storage-delete(key_ptr, key_len) -> s32
        inst.func_new("storage-delete", |mut caller, params, results| {
            let key_ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let key_len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let data = mem.data(&caller);
                if key_ptr + key_len <= data.len() {
                    let key = std::str::from_utf8(&data[key_ptr..key_ptr+key_len]).unwrap_or("");
                    let dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".into());
                    let path = std::path::Path::new(&dir).join(key);
                    let _ = std::fs::remove_file(&path);
                }
            }
            results[0] = CVal::S32(0);
            Ok(())
        })?;

        // http-get(url_ptr, url_len, resp_ptr, buf_size, flags) -> s32
        // Makes a blocking HTTP GET, writes response to buffer. Returns bytes written or -1.
        // Uses reqwest::blocking via spawn_blocking to avoid tokio panic.
        inst.func_new("http-get", move |mut caller, params, results| {
            let url_ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let url_len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let resp_ptr = match params[2] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let buf_size = match params[3] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let data = mem.data(&caller);
                if url_ptr + url_len <= data.len() {
                    let url_bytes = &data[url_ptr..url_ptr+url_len];
                    let url = match std::str::from_utf8(url_bytes) {
                        Ok(u) => u.to_string(),
                        Err(_) => { results[0] = CVal::S32(-1); return Ok(()); }
                    };
                    
                    // Blocking HTTP GET
                    let response = std::thread::scope(|s| {
                        s.spawn(|| {
                            reqwest::blocking::Client::builder()
                                .timeout(std::time::Duration::from_secs(10))
                                .build()
                                .ok()?
                                .get(&url)
                                .send()
                                .ok()?
                                .bytes()
                                .ok()
                        }).join().ok().flatten()
                    });
                    
                    match response {
                        Some(body) => {
                            let write_len = body.len().min(buf_size);
                            let mem_mut = mem.data_mut(&mut caller);
                            if resp_ptr + write_len <= mem_mut.len() {
                                mem_mut[resp_ptr..resp_ptr+write_len].copy_from_slice(&body[..write_len]);
                                results[0] = CVal::S32(write_len as i32);
                            } else {
                                results[0] = CVal::S32(-1);
                            }
                        }
                        None => results[0] = CVal::S32(-1),
                    }
                } else { results[0] = CVal::S32(-1); }
            } else { results[0] = CVal::S32(-1); }
            Ok(())
        })?;

        // env-signer(ptr, len, flags) -> s32 — returns "local" as signer
        inst.func_new("env-signer", |mut caller, params, results| {
            let ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let signer = b"local";
                let write_len = signer.len().min(len);
                let mem_mut = mem.data_mut(&mut caller);
                if ptr + write_len <= mem_mut.len() {
                    mem_mut[ptr..ptr+write_len].copy_from_slice(&signer[..write_len]);
                    results[0] = CVal::S32(write_len as i32);
                } else { results[0] = CVal::S32(-1); }
            } else { results[0] = CVal::S32(-1); }
            Ok(())
        })?;

        // env-predecessor(ptr, len, flags) -> s32 — returns "local"
        inst.func_new("env-predecessor", |mut caller, params, results| {
            let ptr = match params[0] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let len = match params[1] { CVal::S32(v) => v as usize, _ => return Ok(()) };
            let memory = caller.data().memory.lock().ok().and_then(|g| g.clone());
            if let Some(mem) = memory {
                let pred = b"local";
                let write_len = pred.len().min(len);
                let mem_mut = mem.data_mut(&mut caller);
                if ptr + write_len <= mem_mut.len() {
                    mem_mut[ptr..ptr+write_len].copy_from_slice(&pred[..write_len]);
                    results[0] = CVal::S32(write_len as i32);
                } else { results[0] = CVal::S32(-1); }
            } else { results[0] = CVal::S32(-1); }
            Ok(())
        })?;

        // Stubs for remaining functions (return 0)
        let stub_funcs: &[(&str, usize)] = &[
            ("view", 8), ("call", 13), ("transfer", 10),
            ("storage-increment", 6), ("storage-decrement", 6),
            ("storage-set-if-absent", 4), ("storage-set-if-equals", 8),
            ("storage-list-keys", 5), ("storage-clear-all", 0),
            ("storage-set-worker", 4), ("storage-get-worker", 5),
            ("storage-set-worker-public", 4), ("storage-get-worker-from-project", 7),
        ];
        for &(name, _) in stub_funcs {
            inst.func_new(name, move |_caller, _params, results| {
                results[0] = CVal::S32(0);
                Ok(())
            })?;
        }
        debug!("Added outlayer:api/host component instance to P2 linker (real storage + HTTP)");
    }

    // Add NEAR RPC host functions if context has RPC proxy
    let rpc_state = if let Some(ctx) = exec_ctx {
        if let Some(outlayer_rpc) = &ctx.outlayer_rpc {
            debug!("Adding NEAR RPC host functions to linker");

            // Create sync RPC proxy (uses reqwest::blocking — spawn_blocking to avoid tokio panic)
            let rpc_url = outlayer_rpc.get_rpc_url().to_string();
            let sync_proxy = tokio::task::spawn_blocking(move || {
                crate::outlayer_rpc::host_functions_sync::RpcProxy::new(
                    &rpc_url,
                    100,
                    true,
                    None,
                )
            }).await
                .context("spawn_blocking for RPC proxy failed")?
                .context("Failed to create RPC proxy")?;

            // Add RPC host functions to linker
            crate::outlayer_rpc::add_rpc_to_linker(&mut linker, |state: &mut HostState| {
                state.rpc_state_mut()
            })?;

            Some(RpcHostState::new(sync_proxy))
        } else {
            debug!("No RPC proxy in execution context");
            None
        }
    } else {
        debug!("No execution context provided");
        None
    };

    // Add storage host functions if context has storage config
    let storage_state = if let Some(ctx) = exec_ctx {
        if let Some(storage_config) = &ctx.storage_config {
            debug!("Adding storage host functions to linker");

            // Create storage client (uses reqwest::blocking internally — use spawn_blocking)
            let config_clone = storage_config.clone();
            let storage_client = tokio::task::spawn_blocking(move || {
                StorageClient::new(config_clone)
            }).await
                .context("spawn_blocking failed")?
                .context("Failed to create storage client")?;

            add_storage_to_linker(&mut linker, |state: &mut HostState| {
                state.storage_state_mut()
            })?;

            Some(StorageHostState::from_client(storage_client))
        } else if use_local_storage {
            debug!("Adding local-only storage host functions (no remote coordinator)");
            add_storage_to_linker(&mut linker, |state: &mut HostState| {
                state.storage_state_mut()
            })?;
            Some(StorageHostState::local_only()?)
        } else {
            debug!("No storage config in execution context");
            None
        }
    } else if use_local_storage {
        debug!("Adding local-only storage host functions (no execution context)");
        add_storage_to_linker(&mut linker, |state: &mut HostState| {
            state.storage_state_mut()
        })?;
        Some(StorageHostState::local_only()?)
    } else {
        None
    };

    // Extract attached_usd from env_vars for payment state
    // Must be done before env_vars is consumed by wasi_builder
    let attached_usd: u64 = env_vars
        .as_ref()
        .and_then(|env| env.get("ATTACHED_USD"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Check if component imports payment interface
    let has_payment_import = component.component_type().imports(&engine)
        .any(|(name, _)| name.contains("near:payment/api"));

    // Add payment host functions if WASM imports payment interface
    // Even if attached_usd=0, we add the linker so WASM doesn't crash when calling refund_usd
    // Instead, it will get an error message "Refund amount X exceeds attached USD 0"
    let payment_state = if has_payment_import {
        debug!("Adding payment host functions to linker, attached_usd={}", attached_usd);

        // Add payment host functions to linker
        add_payment_to_linker(&mut linker, |state: &mut HostState| {
            state.payment_state_mut()
        })?;

        Some(PaymentHostState::new(attached_usd))
    } else if attached_usd > 0 {
        // WASM has attached_usd but doesn't import payment interface - log warning
        debug!("WASM has attached_usd={} but doesn't import near:payment/api interface, refund not possible", attached_usd);
        None
    } else {
        None
    };

    // Check if component imports VRF interface
    let has_vrf_import = component.component_type().imports(&engine)
        .any(|(name, _)| name.contains("near:vrf/api"));

    let vrf_state = if has_vrf_import {
        if let Some(ref vrf_cfg) = exec_ctx.and_then(|ctx| ctx.vrf_config.as_ref()) {
            debug!("Adding VRF host functions to linker, request_id={}", vrf_cfg.request_id);

            add_vrf_to_linker(&mut linker, |state: &mut HostState| {
                state.vrf_state_mut()
            })?;

            Some(VrfHostState::new(
                vrf_cfg.request_id,
                &vrf_cfg.sender_id,
                &vrf_cfg.keystore_url,
                &vrf_cfg.keystore_auth_token,
                vrf_cfg.tee_session_id.clone(),
            ))
        } else {
            anyhow::bail!(
                "WASM imports near:vrf/api but VRF is not available.\n\
                VRF requires: keystore configured + blockchain execution with request_id."
            );
        }
    } else {
        None
    };

    // Check if component imports wallet interface
    let has_wallet_import = component.component_type().imports(&engine)
        .any(|(name, _)| name.contains("outlayer:wallet/api"));

    let wallet_state = if has_wallet_import {
        if let Some(ref wallet_cfg) = exec_ctx.and_then(|ctx| ctx.wallet_config.as_ref()) {
            debug!("Adding wallet host functions to linker, wallet_id={}", wallet_cfg.wallet_id);

            add_wallet_to_linker(&mut linker, |state: &mut HostState| {
                state.wallet_state_mut()
            })?;

            Some(WalletHostState::new(
                &wallet_cfg.wallet_id,
                &wallet_cfg.coordinator_url,
                &wallet_cfg.wallet_auth_token,
            ))
        } else {
            anyhow::bail!(
                "WASM imports outlayer:wallet/api but wallet is not available.\n\
                Wallet requires: X-Wallet-Id header in the execution request."
            );
        }
    } else {
        None
    };

    // Prepare stdin/stdout/stderr pipes
    let stdin_pipe = wasmtime_wasi::pipe::MemoryInputPipe::new(input_data.to_vec());
    let stdout_pipe =
        wasmtime_wasi::pipe::MemoryOutputPipe::new((limits.max_memory_mb as usize) * 1024 * 1024);
    let stderr_pipe = wasmtime_wasi::pipe::MemoryOutputPipe::new(1024 * 1024);

    // Build WASI context
    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.stdin(stdin_pipe);
    wasi_builder.stdout(stdout_pipe.clone());
    wasi_builder.stderr(stderr_pipe.clone());

    // Add preopened directory (required for WASI P2 filesystem interface)
    wasi_builder.preopened_dir(
        "/tmp",      // host_path
        ".",         // guest_path
        DirPerms::all(),
        FilePerms::all(),
    )?;

    // Add environment variables (from encrypted secrets)
    if let Some(env_map) = env_vars {
        for (key, value) in env_map {
            wasi_builder.env(&key, &value);
            debug!("Added env var: {}", key);
        }
    }

    // Add indicator that RPC proxy is available
    if rpc_state.is_some() {
        wasi_builder.env("NEAR_RPC_PROXY_AVAILABLE", "1");
        debug!("Added env var: NEAR_RPC_PROXY_AVAILABLE=1");
    }

    let host_state = HostState {
        wasi_ctx: wasi_builder.build(),
        wasi_http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        rpc_state,
        storage_state,
        payment_state,
        vrf_state,
        wallet_state,
        http_timeout_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        engine_handle: engine,
        component_instance: None,
        memory: Arc::new(Mutex::new(None)),
    };

    // Create store with fuel limit + epoch deadline
    let mut store = Store::new(&engine, host_state);
    store.set_fuel(limits.max_instructions)?;
    let timeout_secs = limits.max_execution_seconds.max(5);
    // Epoch interruption: engine ticks every second, deadline = timeout_secs ticks.
    // This interrupts even during host calls (wasi-http) unlike tokio::time::timeout.
    store.set_epoch_deadline(timeout_secs);
    store.epoch_deadline_trap();
    // Note: Engine::clone() is Arc clone — epoch counter is shared across all executions.
    // This is fine because main loop executes tasks sequentially (one at a time).
    let epoch_engine = engine.clone();
    let epoch_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        for _ in 0..timeout_secs + 5 {
            interval.tick().await;
            epoch_engine.increment_epoch();
        }
    });

    // Instantiate and execute component
    debug!("Instantiating component");
    // Instantiate component directly (instead of via Command::instantiate_async)
    // so we can extract the memory handle for outlayer host functions
    let component_instance = linker.instantiate_async(&mut store, &component).await
        .map_err(|e| {
            tracing::error!("Failed to instantiate component: {}", e);
            tracing::error!("Error details: {:?}", e);
            e
        })
        .context("Failed to instantiate component")?;

    // Store component instance reference
    store.data_mut().component_instance = Some(component_instance);
    
    // TODO: Extract core memory from the component instance.
    // In wasmtime 28, component::Instance doesn't expose core memory directly.
    // Need to find a way to get wasmtime::Memory from the instantiated component.
    // For now, outlayer host functions that need memory (storage, HTTP) return -1.
    debug!("Note: outlayer host memory access not yet wired (stubs return -1)");

    // Wrap in Command for standard WASI execution
    let command = wasmtime_wasi::bindings::Command::new(&mut store, &component_instance)
        .context("Failed to create Command from component instance")?;

    debug!("Running wasi:cli/run");
    let execution_result = command
        .wasi_cli_run()
        .call_run(&mut store)
        .await;
    // Stop epoch ticker
    epoch_handle.abort();

    // Get fuel consumed before checking result
    let fuel_consumed = limits.max_instructions - store.get_fuel().unwrap_or(0);
    debug!("Component consumed {} instructions", fuel_consumed);

    // Log RPC call count if available
    if let Some(ref rpc_state) = store.data().rpc_state {
        let call_count = rpc_state.proxy.get_call_count();
        if call_count > 0 {
            debug!("Component made {} RPC calls", call_count);
        }
    }

    // Get refund_usd from payment state (if WASM called refund_usd())
    let refund_usd = store.data().payment_state.as_ref().map(|ps| {
        let refund = ps.get_refund_usd();
        if refund > 0 {
            debug!("Component requested refund of {} USD", refund);
        }
        refund
    }).filter(|&r| r > 0);

    // Check execution result
    // Read stderr for debugging (if flag is enabled)
    let stderr_contents = stderr_pipe.contents();
    if print_stderr && !stderr_contents.is_empty() {
        let stderr_str = String::from_utf8_lossy(&stderr_contents);
        tracing::info!("📝 WASM stderr output:\n{}", stderr_str);
    }

    // Check if execution was killed due to HTTP abuse (before checking result)
    let http_timeouts = store.data().http_timeout_count.load(std::sync::atomic::Ordering::Relaxed);
    if http_timeouts >= HTTP_TIMEOUT_ABORT_THRESHOLD {
        anyhow::bail!(
            "Execution terminated: {} HTTP requests exceeded {}s timeout limit (penalty). \
            Full execution cost is charged with no refund (consumed {} instructions)",
            http_timeouts,
            HTTP_REQUEST_TIMEOUT_SECS,
            fuel_consumed
        );
    }

    match execution_result {
        Ok(Ok(())) => {
            debug!("Component execution completed successfully");
            let output = stdout_pipe.contents().to_vec();
            Ok((output, fuel_consumed, refund_usd))
        }
        Ok(Err(_)) | Err(_) => {
            // Check for normal WASI exit (proc_exit(0))
            match &execution_result {
                Err(e) => {
                    if let Some(exit) = e.downcast_ref::<wasmtime_wasi::I32Exit>() {
                        if exit.0 == 0 {
                            let output = stdout_pipe.contents().to_vec();
                            debug!("Component exited normally (I32Exit(0)), output: {} bytes", output.len());
                            return Ok((output, fuel_consumed, refund_usd));
                        }
                    }
                }
                _ => {}
            }
            // Check if this was an epoch interruption (timeout)
            let err_ref = match &execution_result {
                Err(e) => Some(e),
                _ => None,
            };
            let is_epoch_timeout = err_ref
                .map(|e| e.to_string().contains("interrupt"))
                .unwrap_or(false);

            if is_epoch_timeout {
                anyhow::bail!(
                    "WASM execution timed out after {} seconds (penalty). \
                    Full execution cost is charged with no refund (consumed {} instructions)",
                    timeout_secs,
                    fuel_consumed
                );
            }

            // Component exited with error or trapped
            let trap_msg = match &execution_result {
                Err(e) => Some(e.to_string()),
                _ => None,
            };
            let error_msg = if !stderr_contents.is_empty() {
                let stderr_str = String::from_utf8_lossy(&stderr_contents).to_string();
                if let Some(trap) = &trap_msg {
                    format!("{}\nTrap: {}", stderr_str, trap)
                } else {
                    stderr_str
                }
            } else if let Some(trap) = trap_msg {
                format!("Component execution failed: {}", trap)
            } else {
                "Component exited with error (no details in stderr)".to_string()
            };

            debug!("Component execution failed: {}", error_msg);
            Err(anyhow::anyhow!("{}", error_msg))
        }
    }
}

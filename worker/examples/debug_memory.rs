/// Standalone debug tool: run a WASM component with mock storage,
/// dump memory after execution to inspect cabi_realloc trace buffer.
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let wasm_path = std::env::args().nth(1).unwrap_or("/tmp/test_unique_ret.wasm".into());
    let input = std::env::args().nth(2).unwrap_or("{}".into());

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.wasm_component_model(true);
    config.consume_fuel(true);

    let engine = wasmtime::Engine::new(&config)?;
    let mut store = wasmtime::Store::new(&engine, ());
    store.set_fuel(10_000_000)?;

    let component = wasmtime::component::Component::from_file(&engine, &wasm_path)?;

    // Create linker with minimal WASI + mock storage
    let mut linker = wasmtime::component::Linker::new(&engine);

    // Mock wasi:cli/run
    wasmtime_wasi::preview2::command::add_to_linker(&mut linker, |_: &mut ()| {
        unreachable!("no state needed")
    })?;

    // Mock near:storage/api
    let storage_data: std::sync::Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));

    // Pre-populate storage for the test
    {
        let mut sd = storage_data.lock().unwrap();
        sd.insert("key1".into(), b"AAAA".to_vec());
        sd.insert("key2".into(), b"BBBB".to_vec());
    }

    linker.root().bind("near:storage/api@0.1.0", "get", 
        move |mut ctx: wasmtime::component::Caller<'_, ()>, key_ptr: i32, key_len: i32, ret_ptr: i32| {
            let mem = ctx.get_export("memory").unwrap().into_memory().unwrap();
            let data = mem.data(&ctx);
            let key = String::from_utf8_lossy(&data[key_ptr as usize..(key_ptr + key_len) as usize]).to_string();
            let sd = storage_data.lock().unwrap();
            let (val, err): (Vec<u8>, String) = match sd.get(&key) {
                Some(v) => (v.clone(), String::new()),
                None => (Vec::new(), String::new()),
            };
            eprintln!("MOCK get({:?}) → {} bytes", key, val.len());
            // Write result to ret_area: [val_ptr, val_len, err_ptr, err_len]
            // But we can't call cabi_realloc from here... this won't work.
            // The canonical lowering expects us to be the host that returns the data.
            // Actually, we're not the host in this model. The LINKER binds component-level functions.
            // The canonical ABI lowering handles allocation.
            Ok::<(), anyhow::Error>(())
        }
    )?;

    println!("Linker setup done. Instantiating...");
    let _instance = linker.instantiate_async(&mut store, &component).await?;
    println!("Instantiated. But we can't easily mock the canonical ABI storage functions this way.");

    Ok(())
}

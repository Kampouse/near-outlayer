---
name: inlayer
description: Local WASM execution engine for OutLayer. Runs WASI/P2 WASM files locally with NEAR RPC proxy, storage, and TEE simulation. Use when: running OutLayer WASM locally, testing before deployment, or as the execution backend for layerd worker.
---

# inlayer — Local WASM Runner

Runs OutLayer-compatible WASM locally. No TEE, no deployment — just execute.

## Binary

Built from `near-outlayer/worker/src/bin/inlayer.rs` (also in `near-outlayer` workspace).

```bash
# Build
cd near-outlayer/worker && cargo build --release --bin inlayer

# Install
cp target/release/inlayer ~/.local/bin/
```

## Usage

```bash
# Run with JSON input
inlayer run ./my-app.wasm '{"action":"stats"}'

# With custom RPC
inlayer run ./my-app.wasm '{"action":"query"}' --rpc https://test.rpc.fastnear.com

# With config file
inlayer run ./my-app.wasm '{}' --config ./inlayer.config
```

## Config

`inlayer.config` (searched in `./`, `~/.inlayer/`):

```toml
[runtime]
wasm_search_paths = ["/path/to/wasm/dir"]
default_input = "{}"

[env]
NEAR_RPC_URL = "https://test.rpc.fastnear.com"
TEE_SIGNER_ID = "test.testnet"
```

## WASM Discovery

Searches `wasm_search_paths` for WASM files. No hardcoded paths — all via config.

## Output Format

```
✅ Success: true
📤 Output: {"success":true,"created_at":0}
Time: 13443ms
Instructions: 13443
```

Parsed by `layerd` to extract success/output/timing.

## Architecture

```
inlayer
├── loads WASM (WASI Preview 2 component)
├── creates execution context (RPC proxy, storage, env vars)
├── runs via wasmtime engine
└── captures stdout → result
```

- **RPC Proxy**: Intercepts WASM HTTP calls → NEAR RPC
- **Storage**: Local filesystem in `./storage/`
- **Env vars**: From config `[env]` section
- **No TEE**: Local execution, no attestation

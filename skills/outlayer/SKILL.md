---
name: outlayer
description: OutLayer — verifiable off-chain computation for NEAR. Deploy WASM code to TEE workers, call via NEAR smart contracts (yield/resume) or HTTPS API. Use when: deploying, managing, or calling OutLayer projects; setting up secrets, payment keys, persistent storage; integrating OutLayer with NEAR contracts; using host functions (call/view/transfer); building WASI P1/P2 applications for TEE execution. Covers both testnet (outlayer.testnet) and mainnet (outlayer.near).
---

# OutLayer

Verifiable off-chain computation for NEAR. Run WASM in Intel TDX with cryptographic attestation.

## Networks

| Network | Contract | Login | HTTPS API |
|---------|----------|-------|-----------|
| **Mainnet** | `outlayer.near` | `outlayer login` (default) | `api.outlayer.fastnear.com` |
| **Testnet** | `outlayer.testnet` | `outlayer login testnet` | Same (auto-detected by project owner) |

**Worker registration contracts**: `worker.outlayer.near` / `worker.outlayer.testnet`

**Key differences:**
- Mainnet: Pay with real NEAR, USDC/USDT for HTTPS API
- Testnet: Free test NEAR, use `--accountId you.testnet`
- Secrets, projects, and payment keys are network-specific
- `outlayer login` defaults to mainnet. Always specify `testnet` explicitly for testnet work.

## Architecture

**Two integration modes:**
- **NEAR Blockchain** — `request_execution` on `outlayer.near` (mainnet) / `outlayer.testnet`. Pay with NEAR, result via callback.
- **HTTPS API** — `POST https://api.outlayer.fastnear.com/call/{owner}/{project}`. Pay with USDC via Payment Keys. Sub-second response.

**Code sources:**
- `GitHub` — `{repo, commit, build_target}`. First call compiles (10-30s), then cached.
- `WasmUrl` — `{url, hash, build_target}`. Pre-compiled WASM on FastFS/IPFS. Instant execution.
- `Project` — `{project_id, version_key}`. Registered project with versioning + storage.

**WASI targets:**
- `wasm32-wasip1` — Simple computations, smaller binaries. No HTTP, no storage.
- `wasm32-wasip2` — HTTP requests, persistent storage, host functions. Required for storage and `outlayer` crate.

## Deployment Workflow

**Always use the CLI for deploying — never raw `near call` or manual API calls.**

### Method 1: Git deploy (from repo)
`outlayer deploy <name>` with no URL auto-detects git origin + HEAD commit.
```bash
cd my-agent/
git push
outlayer deploy my-agent               # Auto: origin remote + HEAD commit
outlayer deploy my-agent --no-activate # Deploy without activating
outlayer deploy my-agent --target wasm32-wasip1  # Override WASI target
```
First call compiles on OutLayer (10-30s), then cached.

### Method 2: Upload + Deploy (recommended for production)
Pre-compile locally, upload to FastFS, deploy with URL. Gives instant execution.
```bash
# Build locally
cargo build --target wasm32-wasip2 --release

# Upload to FastFS
outlayer upload target/wasm32-wasip2/release/my_agent.wasm
# → FastFS URL + SHA256 hash

# Deploy with FastFS URL
outlayer deploy my-agent <fastfs_url>
outlayer deploy my-agent <fastfs_url> --hash <sha256>  # Explicit hash
```

### Safe rollout pattern
```bash
outlayer deploy my-agent <fastfs_url> --no-activate    # Deploy inactive
outlayer run my-agent --version <version_key> '{}'      # Test specific version
outlayer versions activate <version_key>                # Go live when ready
```

**Why upload + deploy**: FastFS = instant execution (no compilation), hash-verified WASM, works with secret binding by WASM hash.

### Project config
`outlayer.toml` in project root (created by `outlayer create`):
```toml
[project]
name = "my-agent"
owner = "alice.near"

[build]
target = "wasm32-wasip2"
source = "github"

[run]
payment_key_nonce = 1
```

## Quick Reference

### NEAR Contract Call
```bash
near call outlayer.testnet request_execution '{
  "source": {"GitHub": {"repo": "https://github.com/user/project", "commit": "main"}},
  "input_data": "{\"key\": \"value\"}",
  "resource_limits": {"max_instructions": 10000000000, "max_memory_mb": 128, "max_execution_seconds": 60},
  "response_format": "Json"
}' --accountId you.testnet --deposit 0.1 --gas 300000000000000
```

### HTTPS API Call
```bash
curl -X POST https://api.outlayer.fastnear.com/call/owner/project \
  -H "X-Payment-Key: owner.near:1:SECRET_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input": {"key": "value"}}'
```

### Smart Contract Integration (Rust)
```rust
Promise::new("outlayer.near".parse().unwrap())
    .function_call("request_execution".into(), json!({
        "source": {"Project": {"project_id": "alice.near/my-app"}},
        "input_data": json!({"city": city}).to_string(),
    }).to_string().into_bytes(), env::attached_deposit(), Gas::from_tgas(100))
    .then(Self::ext(env::current_account_id())
        .with_static_gas(Gas::from_tgas(10))
        .on_outlayer_result())
```

## WASM Code Structure

- **Input**: Read JSON from `stdin`
- **Output**: Write JSON to `stdout` (≤900 bytes for NEAR callback)
- **Secrets**: `std::env::var("SECRET_NAME")` — injected at runtime
- **Must use `[[bin]]`** in Cargo.toml with `fn main()`, not `[lib]`
- **Flush stdout**: `io::stdout().flush().unwrap()`

## Host Functions (wasm32-wasip2 only)

Available via `outlayer` crate on crates.io. WIT interface at `near:api@0.1.0`:

- `view(contract_id, method_name, args_json) → (result, status)` — Read-only query
- `call(signer_id, signer_key, receiver_id, method_name, args_json, deposit_yocto, gas) → (tx_hash, status)` — Contract call
- `transfer(signer_id, signer_key, receiver_id, amount_yocto) → (tx_hash, status)` — NEAR transfer

Signer credentials provided via secrets. Private RPC access via Fastnear.

## Environment Variables in WASM

| Variable | NEAR | HTTPS |
|----------|------|-------|
| `OUTLAYER_EXECUTION_TYPE` | "NEAR" | "HTTPS" |
| `NEAR_SENDER_ID` | Tx signer | Payment Key owner |
| `USD_PAYMENT` | "0" | X-Attached-Deposit (micro-USD) |
| `NEAR_PAYMENT_YOCTO` | Attached NEAR | "0" |
| `OUTLAYER_PROJECT_ID` | owner/name | owner/name |
| `OUTLAYER_PROJECT_UUID` | UUID (if project) | UUID (if project) |

## Secrets

- **Manual**: Key-value pairs encrypted client-side (ChaCha20-Poly1305). Set via dashboard or CLI.
- **Protected (CKD)**: `PROTECTED_*` prefix — generated in TEE, nobody sees the value. For signing keys, derivation keys.
- **Binding**: By repo+branch, by WASM hash, or by project. Project binding recommended (survives code updates).
- **Access in code**: `std::env::var("KEY_NAME")`

**⚠️ Known issue**: CLI `outlayer secrets set` may not properly inject at runtime. Web dashboard secrets work more reliably.

## Payment Keys (HTTPS API)

Format: `X-Payment-Key: owner:nonce:secret`
- Prepaid USD balance (USDC/USDT)
- Can restrict to specific projects (`project_ids`)
- Spending limit per call (`max_per_call`)
- Top up via `ft_transfer_call` to `outlayer.near`

## Projects

- ID format: `owner.near/project-name`
- Version management: multiple versions, switch active anytime
- Persistent storage (WASI P2 only): `outlayer::storage::set/get`
- Storage encrypted, user-isolated, shared across versions
- Create via dashboard at `/projects`

## FastFS Workflow (Production)

1. Compile with `store_on_fastfs: true, compile_only: true`
2. Get FastFS URL + WASM hash from response
3. Use `WasmUrl` source for instant execution
4. **Note**: FastFS propagation can take >60s — retry on 404

## Pricing

- Pay per execution: base fee + (instructions × fee) + (time × fee)
- Unused deposit auto-refunded
- No refund on execution failure (anti-DoS)
- HTTPS: charged in micro-USD (1000000 = $1.00)

## TEE Attestation

Every execution produces Intel TDX attestation proving:
- Code integrity (WASM SHA256)
- Input/output integrity (SHA256)
- Worker identity (registered measurements MRTD + RTMR0-3)
- Task Hash in REPORTDATA prevents attestation forgery

Verify at `/executions` → "View Attestation" or `GET /attestation/{job_id}`

## Known Gotchas

- **900 byte output limit** for NEAR callbacks (stdout). Plan accordingly.
- **WASI P1 vs P2**: Use P2 for HTTP, storage, host functions. P1 for simple computations only.
- **FastFS delay**: Wait >60s after upload before deploying.
- **Secrets injection**: Use project-bound secrets (`--project owner/name`) — they persist across code updates. WASM hash-bound secrets need re-creation on each deploy.
- **`cabi_realloc` error on P2**: Project not configured as WASI P2 component.
- **GitHub first run**: 10-30s compilation, then cached.

## CLI Tool

The `outlayer` CLI manages projects, secrets, deployments, and payments. Key commands:

```bash
outlayer login                              # Auth with NEAR key or wallet key (wk_...)
outlayer create my-agent                    # New project from template
outlayer deploy my-agent                    # Deploy from git repo
outlayer run alice.near/my-agent '{}'       # Execute
outlayer secrets set '{"KEY":"val"}' --project owner/name
outlayer keys create                        # Payment key for HTTPS API
```

**Full CLI reference**: See [references/cli.md](references/cli.md)

## layerd — Autonomous Worker

Polls the OutLayer contract for pending requests, executes WASM via `inlayer`, submits results on-chain. Runs as a daemon.

**Repo**: `near-outlayer/worker/src/bin/layerd.rs` (branch `inlayer` on `Kampouse/near-outlayer`)

### Setup

```bash
cd near-outlayer/worker
cargo build --release --bin layerd --bin inlayer
mkdir -p ~/.inlayer/bin
cp target/release/layerd target/release/inlayer ~/.inlayer/bin/

# Configure (optional)
cat > ~/.inlayer/layerd.config << 'EOF'
rpc_url = "https://test.rpc.fastnear.com"
poll_interval_secs = 10
EOF

# Run
layerd                          # foreground
layerd --daemon                 # background
layerd --start                  # via launchd (auto-restart)
layerd --stop                   # stop
layerd --status                 # check
layerd --log                    # tail log
```

### Architecture

```
User ──(tx)──▶ NEAR Contract ──(poll)──▶ layerd ──(exec)──▶ inlayer
                    │                         │
                    │◀──(resolve tx)──────────┘
```

- **No `near` CLI dependency** — direct RPC via `near-jsonrpc-client`
- **Contract IS the queue** — no databases, no webhooks
- **Exponential backoff** on RPC errors (max 5 min)
- **WASM discovery** — searches configured dirs for `.wasm` files

### Config

| Field | Default | Description |
|-------|---------|-------------|
| `contract_id` | `outlayer.kampouse.testnet` | NEAR contract |
| `account_id` | `kampouse.testnet` | Signer account |
| `network` | `testnet` | NEAR network |
| `rpc_url` | `https://test.rpc.fastnear.com` | RPC endpoint |
| `poll_interval_secs` | `5` | Poll frequency |
| `wasm_search_dirs` | `~/.openclaw/workspace` | WASM search paths |
| `key_path` | `~/.near-credentials/testnet/<account>.json` | Signer key |
| `log_file` | `~/.inlayer/layerd.log` | Log path |
| `pid_file` | `~/.inlayer/layerd.pid` | PID path |

### macOS launchd Service

```bash
# Install
cp scripts/com.outlayer.layerd.plist ~/Library/LaunchAgents/
# Edit paths in plist for your setup
layerd --start    # auto-start on boot, restarts on crash
layerd --stop     # unload launchd (stays stopped)
```

### E2E Test

```bash
# Submit request
near contract call-function as-transaction outlayer.kampouse.testnet request_execution \
  json-args '{"source":{"WasmUrl":{"url":"https://example.com/test.wasm","hash":"0000000000000000000000000000000000000000000000000000000000000000","build_target":"wasm32-wasip2"}},"resource_limits":{"max_instructions":500000000000,"max_memory_mb":256,"max_execution_seconds":60},"input_data":"eyJhY3Rpb24iOiJzdGF0cyJ9"}' \
  prepaid-gas '100.0 Tgas' attached-deposit '0.01 NEAR' \
  sign-as kampouse.testnet network-config testnet sign-with-keychain send

# Watch
layerd --log
```

### RPC Endpoints

⚠️ `rpc.testnet.near.org` is deprecated and rate-limited. Use:
- **Testnet**: `https://test.rpc.fastnear.com`
- **Mainnet**: `https://rpc.fastnear.com`

## inlayer — Local WASM Runner

Runs OutLayer WASM locally for testing. No TEE, no deployment.

```bash
inlayer run ./app.wasm '{"action":"stats"}' --rpc https://test.rpc.fastnear.com
```

See `skills/inlayer/SKILL.md` for full details.

## Detailed References

- **CLI full reference**: See [references/cli.md](references/cli.md)
- **HTTPS API full spec**: See [references/https-api.md](references/https-api.md)
- **Examples & patterns**: See [references/examples.md](references/examples.md)

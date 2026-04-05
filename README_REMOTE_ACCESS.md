# Remote Access Guide for inlayer Daemon

## Quick Setup Options

### Option 1: ngrok (Development/Easiest)
```bash
# 1. Start daemon on all interfaces
inlayer daemon --foreground --dashboard 0.0.0.0:8082

# 2. Create tunnel in another terminal
ngrok http 8082

# 3. Use the public URL ngrok provides
# Example: https://abc123.ngrok.io/call/myaccount/myproject
```

### Option 2: Direct Exposure (Simple)
```bash
# 1. Configure daemon to bind to all interfaces
# Add to inlayer.config:
dashboard_addr = "0.0.0.0:8082"

# 2. Find your local IP
ipconfig getifaddr en0  # macOS
# or
ip addr show           # Linux

# 3. Forward port 8082 on your router
# External Port: 8082 → Internal Port: 8082

# 4. Access via your public IP
# http://your-public-ip:8082/call/myaccount/myproject
```

### Option 3: Nginx + SSL (Production)
See `nginx.conf.example` for full configuration.

## Security Best Practices

### 1. Add API Key Authentication
Edit `daemon.rs` to add API key middleware:

```rust
// Add to api_call function in daemon.rs
let api_key = state.api_key.clone();
if let Some(key) = headers.get("X-API-Key") {
    if key != &api_key {
        return (StatusCode::UNAUTHORIZED, json({"error": "Invalid API key"}));
    }
} else {
    return (StatusCode::UNAUTHORIZED, json({"error": "Missing API key"}));
}
```

### 2. Add Rate Limiting
```rust
// Add to Cargo.toml
tower-governor = "0.4"

// Use in your axum router
use tower_governor::{Governor, GovernorConfigBuilder};

let governor_conf = Box::new(
    GovernorConfigBuilder::default()
        .per_second(10)
        .burst_size(30)
        .finish()
        .unwrap(),
);
```

### 3. Use HTTPS Only
Never expose the dashboard over plain HTTP in production.

### 4. Network Segmentation
- Run daemon on isolated network/VLAN
- Use firewall rules to restrict access
- Only expose port 443 (via nginx), not 8082 directly

## Client Usage

### From Remote Client
```bash
# Using curl
curl -X POST https://your-domain.com/call/myaccount/myproject \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '{"input": {"your": "data"}}'

# Using the OutLayer SDK/CLI
# Update the coordinator URL in your client config
coordinator_url = "https://your-domain.com"
```

### Monitor Dashboard
```bash
# Access dashboard (if enabled)
https://your-domain.com/api/status
https://your-domain.com/api/history
https://your-domain.com/api/stream  # SSE events
```

## Troubleshooting

### Connection Refused
```bash
# Check if daemon is running
inlayer daemon --status

# Check if port is open
netstat -an | grep 8082
# or
lsof -i :8082
```

### Router Port Forwarding
```bash
# Test from local network first
curl http://192.168.1.x:8082/api/status

# Then test from external network
curl http://your-public-ip:8082/api/status
```

### Firewall Rules
```bash
# Allow port 8082 (testing only)
sudo ufw allow 8082

# Allow nginx (production)
sudo ufw allow 'Nginx Full'
```

## Dynamic DNS (Optional)

If you don't have a static IP:

```bash
# Install ddclient
sudo apt install ddclient

# Configure with your DNS provider
# (DuckDNS, No-IP, Cloudflare, etc.)
```

## Examples

### Test from external client:
```bash
# Simple health check
curl https://your-domain.com/api/status

# Execute WASM remotely
curl -X POST https://your-domain.com/call/myaccount/myproject \
  -H "Content-Type: application/json" \
  -d '{"input": {"command": "hello"}}'
```

### Monitor in real-time:
```bash
# Server-side - watch logs
tail -f ~/.inlayer/layerd.log

# Client-side - SSE stream
curl https://your-domain.com/api/stream
```

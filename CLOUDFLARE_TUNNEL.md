# Cloudflare Tunnel Guide for inlayer Daemon

## Why Cloudflare Tunnel is Better Than ngrok

✅ **Free forever** (ngrok has limits)
✅ **Faster** (Cloudflare's global network)
✅ **No router config** (no port forwarding needed)
✅ **Automatic HTTPS** (built-in SSL certificates)
✅ **Custom domains** (use your own domain name)
✅ **DDoS protection** (Cloudflare's network)
✅ **Production-ready** (designed for production use)

## Quick Start (2 minutes)

### Terminal 1: Start inlayer daemon
```bash
inlayer daemon --foreground --dashboard 127.0.0.1:8082
```

### Terminal 2: Create Cloudflare tunnel
```bash
cloudflared tunnel --url http://localhost:8082
```

**Output:**
```
2024-04-05T12:00:00Z INFO Starting tunnel
2024-04-05T12:00:01Z INFO Your quick Tunnel has been created!
Visit it at: https://abc-def-123.trycloudflare.com
```

### Terminal 3: Test it
```bash
# Check status
curl https://abc-def-123.trycloudflare.com/api/status

# Execute WASM remotely
curl -X POST https://abc-def-123.trycloudflare.com/call/myaccount/project \
  -H "Content-Type: application/json" \
  -d '{"input": {"test": "data"}}'
```

## Production Setup (Custom Domain)

### 1. Create Named Tunnel
```bash
cloudflared tunnel create inlayer-daemon
```

**Output:**
```
Tunnel ID: abc123-def456-ghi789
Created credentials file in /root/.cloudflared/abc123-def456-ghi789.json
```

### 2. Configure Tunnel
Create `~/.cloudflared/config.yml`:
```yaml
tunnel: abc123-def456-ghi789
credentials-file: /root/.cloudflared/abc123-def456-ghi789.json

ingress:
  # Main endpoint - your custom domain
  - hostname: inlayer.your-domain.com
    service: http://localhost:8082

  # Fallback for other requests
  - service: http_status:404
```

### 3. Authenticate with Cloudflare
```bash
cloudflared tunnel token
```
Visit the URL shown and authorize with your Cloudflare account.

### 4. Add DNS Record
In Cloudflare Dashboard:
1. Go to DNS > Records
2. Add CNAME record:
   - Name: `inlayer`
   - Target: `abc123-def456-ghi789.cfarg.net`
   - Proxy: ✅ (orange cloud)

### 5. Run Tunnel
```bash
cloudflared tunnel run inlayer-daemon
```

### 6. Setup as Service (Linux)
Create `/etc/systemd/system/cloudflared-inlayer.service`:
```ini
[Unit]
Description=Cloudflare Tunnel for inlayer daemon
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/cloudflared tunnel run inlayer-daemon
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable cloudflared-inlayer
sudo systemctl start cloudflared-inlayer
```

### 7. Setup as Service (macOS)
```bash
# Install cloudflared as brew service
brew services start cloudflared

# Or use launchd
sudo cp /opt/homebrew/Cellar/cloudflared/*/com.cloudflare.cloudflared.plist /Library/LaunchDaemons/
sudo launchctl load -w /Library/LaunchDaemons/com.cloudflare.cloudflared.plist
```

## Security Best Practices

### 1. Restrict Access by Country
In Cloudflare Dashboard > Security > WAF:
- Create rule to only allow specific countries
- Or block countries you don't need

### 2. Add API Key Authentication
Add to `inlayer/config` or use environment variable:
```toml
# inlayer.config
api_key = "your-secret-api-key-here"
```

Then require it in API calls:
```bash
curl -X POST https://inlayer.your-domain.com/call/account/project \
  -H "X-API-Key: your-secret-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"input": {"test": "data"}}'
```

### 3. Enable Cloudflare Access (Zero Trust)
```bash
# Install cloudflared access
cloudflared access login --url https://inlayer.your-domain.com
```

Then require authentication in Cloudflare Dashboard:
- Zero Trust > Access > Applications
- Add application: https://inlayer.your-domain.com
- Set up: Email, Google Auth, or One-Time PIN

### 4. Rate Limiting
In Cloudflare Dashboard > Security > WAF:
- Create rule to limit requests per minute
- Example: `http.request.uri.path matches "^/call/*"` → Rate limit: 100/minute

## Monitoring

### View Tunnel Metrics
```bash
# View tunnel info
cloudflared tunnel info inlayer-daemon

# List all tunnels
cloudflared tunnel list

# View tunnel metrics (requires Cloudflare account)
# Visit: https://dash.cloudflare.com/ > Zero Trust > Networks
```

### Check Tunnel Health
```bash
# Test endpoint
curl https://inlayer.your-domain.com/api/status

# Monitor logs
cloudflared tunnel run inlayer-daemon --loglevel debug
```

## Troubleshooting

### Tunnel Not Starting
```bash
# Check if cloudflared is running
ps aux | grep cloudflared

# Check if port 8082 is available
lsof -i :8082

# Test local connection
curl http://localhost:8082/api/status
```

### DNS Not Propagating
```bash
# Check DNS
dig inlayer.your-domain.com

# Should show:
# abc123-def456-ghi789.cfarg.net

# If not, wait a few minutes for DNS propagation
```

### Connection Refused
```bash
# Make sure daemon is running
inlayer daemon --status

# Check tunnel is running
cloudflared tunnel info inlayer-daemon

# Check firewall (should not be needed for Cloudflare)
sudo ufw status
```

### SSL Certificate Issues
Cloudflare handles SSL automatically, but make sure:
- SSL/TLS is set to "Full" in Cloudflare Dashboard
- Origin is using HTTP (localhost:8082)
- Cloudflare presents SSL to the world

## Comparison: Cloudflare Tunnel vs Alternatives

| Feature | Cloudflare | ngrok | nginx + port forwarding |
|---------|-----------|-------|------------------------|
| Cost | Free | Paid tier needed | Free (but need domain) |
| Speed | Fast | Slower | Fastest |
| Setup | 2 min | 2 min | 30 min |
| Custom Domain | ✅ Free | ❌ Paid only | ✅ Free |
| HTTPS | ✅ Automatic | ✅ Automatic | Manual (Let's Encrypt) |
| Router Config | ❌ Not needed | ❌ Not needed | ✅ Required |
| DDoS Protection | ✅ Built-in | ❌ No | ❌ No |
| Production Ready | ✅ Yes | ❌ No | ✅ Yes |

## Advanced Configuration

### Multiple Tunnels
```bash
# Create tunnels for different environments
cloudflared tunnel create inlayer-dev
cloudflared tunnel create inlayer-prod
```

### Load Balancing
```yaml
ingress:
  - hostname: inlayer.your-domain.com
    service: http://localhost:8082

  - hostname: inlayer-prod.your-domain.com
    service: http://localhost:8083

  - service: http_status:404
```

### WebSocket Support
Cloudflare tunnels support WebSockets automatically (great for SSE events at `/api/stream`).

### IP Restrictions
```yaml
ingress:
  - hostname: inlayer.your-domain.com
    service: http://localhost:8082
    # Restrict to specific IPs
    originRequest:
      ipRules:
        - value: "1.2.3.4/32"
          action: allow
        - action: block
```

## Example: Complete Setup Script

```bash
#!/bin/bash
# Complete Cloudflare Tunnel setup for inlayer

# 1. Start inlayer daemon
echo "Starting inlayer daemon..."
inlayer daemon --foreground --dashboard 127.0.0.1:8082 &
DAEMON_PID=$!

# 2. Wait for daemon to start
sleep 3

# 3. Create quick tunnel
echo "Creating Cloudflare tunnel..."
cloudflared tunnel --url http://localhost:8082 &
TUNNEL_PID=$!

# 4. Wait for tunnel URL
sleep 5

echo ""
echo "✅ Setup complete!"
echo ""
echo "Your inlayer daemon is now accessible from anywhere!"
echo ""
echo "Test it:"
echo "  curl https://your-tunnel-url.trycloudflare.com/api/status"
echo ""
echo "Press Ctrl+C to stop"

# Cleanup
trap "kill $DAEMON_PID $TUNNEL_PID 2>/dev/null" EXIT
wait
```

## Links

- Cloudflare Tunnel Docs: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/
- cloudflared GitHub: https://github.com/cloudflare/cloudflared
- Cloudflare Dashboard: https://dash.cloudflare.com/

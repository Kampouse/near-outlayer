#!/bin/bash
# Quick Cloudflare Tunnel for inlayer (automatic URL extraction)

echo "🚀 Starting inlayer with Cloudflare Tunnel..."
echo ""

# Start daemon (use --status mode to avoid config validation issues)
echo "1️⃣  Starting inlayer daemon..."
./worker/target/debug/inlayer daemon --foreground --dashboard 127.0.0.1:8082 > /tmp/inlayer.log 2>&1 &
DAEMON_PID=$!
echo "   ✅ Daemon PID: $DAEMON_PID"

# Wait a bit
sleep 2

# Start cloudflared and capture URL
echo "2️⃣  Creating Cloudflare tunnel (this takes ~10 seconds)..."
cloudflared tunnel --url http://localhost:8082 2>&1 | tee /tmp/cloudflared.log &
CLOUDFLARED_PID=$!

# Wait for URL to appear
echo "   ⏳ Waiting for tunnel URL..."
for i in {1..15}; do
    TUNNEL_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' /tmp/cloudflared.log 2>/dev/null | head -1)
    if [ -n "$TUNNEL_URL" ]; then
        echo ""
        echo "🎉 TUNNEL IS LIVE!"
        echo ""
        echo "📍 URL: $TUNNEL_URL"
        echo ""
        echo "📡 Test it:"
        echo "   curl $TUNNEL_URL/api/status"
        echo ""
        echo "🛑 Stop with: kill $CLOUDFLARED_PID $DAEMON_PID"
        echo ""
        echo "✨ Press Ctrl+C to stop"

        # Keep running until interrupted
        trap "echo ''; echo '🛑 Stopping...'; kill $CLOUDFLARED_PID 2>/dev/null; kill $DAEMON_PID 2>/dev/null; echo '✅ Stopped'; exit 0" INT TERM
        wait
        exit 0
    fi
    sleep 1
    echo -n "."
done

echo ""
echo "❌ Timeout - check logs: cat /tmp/cloudflared.log"
kill $CLOUDFLARED_PID 2>/dev/null
kill $DAEMON_PID 2>/dev/null
exit 1

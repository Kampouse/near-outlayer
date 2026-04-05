#!/bin/bash
# Automatically start inlayer daemon with Cloudflare tunnel and extract URL

set -e

# Find the inlayer binary
INLAYER_BIN=""
if [ -f "./worker/target/debug/inlayer" ]; then
    INLAYER_BIN="./worker/target/debug/inlayer"
elif [ -f "../worker/target/debug/inlayer" ]; then
    INLAYER_BIN="../worker/target/debug/inlayer"
elif command -v inlayer &> /dev/null; then
    INLAYER_BIN="inlayer"
else
    echo "❌ Cannot find inlayer binary"
    echo "   Build it first: cd worker && cargo build --bin inlayer"
    exit 1
fi

echo "🚀 Starting inlayer daemon with Cloudflare Tunnel..."
echo "   Using: $INLAYER_BIN"
echo ""

# Check if inlayer daemon is already running
if pgrep -f "inlayer daemon" > /dev/null; then
    echo "⚠️  inlayer daemon is already running"
    echo "   Stopping it first..."
    pkill -f "inlayer daemon" || true
    sleep 2
fi

# Step 1: Start inlayer daemon
echo "1️⃣  Starting inlayer daemon on port 8082..."
$INLAYER_BIN daemon --foreground --dashboard 127.0.0.1:8082 > /tmp/inlayer-daemon.log 2>&1 &
DAEMON_PID=$!
echo "   ✅ Daemon started with PID: $DAEMON_PID"

# Wait for daemon to be ready
echo "   ⏳ Waiting for daemon to start..."
sleep 3

# Check if daemon started successfully
if ! kill -0 $DAEMON_PID 2>/dev/null; then
    echo "   ❌ Failed to start daemon. Check logs:"
    cat /tmp/inlayer-daemon.log
    exit 1
fi

echo "   ✅ Daemon is ready"
echo ""

# Step 2: Start Cloudflare tunnel
echo "2️⃣  Creating Cloudflare tunnel..."
echo "   ⏳ This may take 10-15 seconds..."

# Start cloudflared and capture output to a file
cloudflared tunnel --url http://localhost:8082 > /tmp/cloudflared.log 2>&1 &
CLOUDFLARED_PID=$!

# Wait for tunnel URL to appear
timeout=20
elapsed=0
while [ $elapsed -lt $timeout ]; do
    # Check if URL is in the log
    TUNNEL_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' /tmp/cloudflared.log 2>/dev/null | head -1)

    if [ -n "$TUNNEL_URL" ]; then
        echo "   ✅ Tunnel created!"
        echo ""
        echo "🎉 SUCCESS! Your inlayer daemon is now live on the internet!"
        echo ""
        echo "📍 Tunnel URL: $TUNNEL_URL"
        echo ""
        echo "📡 Available endpoints:"
        echo "   • Status:    $TUNNEL_URL/api/status"
        echo "   • Execute:  $TUNNEL_URL/call/<account>/<project>"
        echo "   • History:  $TUNNEL_URL/api/history"
        echo "   • Stream:   $TUNNEL_URL/api/stream"
        echo ""
        echo "💡 Test it now:"
        echo "   curl $TUNNEL_URL/api/status"
        echo ""
        echo "🛑 Stop the tunnel:"
        echo "   kill $CLOUDFLARED_PID"
        echo "   kill $DAEMON_PID"
        echo ""

        # Save URLs to file for easy reference
        cat > /tmp/inlayer-tunnel-info.txt <<EOF
Inlayer Daemon + Cloudflare Tunnel
=====================================

Tunnel URL: $TUNNEL_URL

Endpoints:
  Status:    $TUNNEL_URL/api/status
  Execute:   $TUNNEL_URL/call/<account>/<project>
  History:   $TUNNEL_URL/api/history
  Stream:    $TUNNEL_URL/api/stream
  Storage:   $TUNNEL_URL/api/storage
  Contract:  $TUNNEL_URL/api/contract

PIDs:
  Daemon:    $DAEMON_PID
  Tunnel:    $CLOUDFLARED_PID

Logs:
  Daemon:    /tmp/inlayer-daemon.log
  Tunnel:    /tmp/cloudflared.log

Stop:
  kill $CLOUDFLARED_PID
  kill $DAEMON_PID
EOF

        echo "📝 Info saved to: /tmp/inlayer-tunnel-info.txt"
        echo ""
        echo "✨ Press Ctrl+C to stop both daemon and tunnel"

        # Wait for user to stop us
        trap "echo ''; echo '🛑 Stopping...'; kill $CLOUDFLARED_PID 2>/dev/null; kill $DAEMON_PID 2>/dev/null; echo '✅ Stopped'; exit 0" INT TERM

        # Show live logs
        echo "📄 Live logs (Ctrl+C to stop):"
        echo ""

        # Follow both log files
        tail -f /tmp/cloudflared.log &
        TAIL_PID=$!

        # Wait for interrupt
        wait

        exit 0
    fi

    sleep 1
    elapsed=$((elapsed + 1))
done

# If we get here, timeout occurred
echo "   ❌ Timeout waiting for tunnel"
echo "   Check logs: cat /tmp/cloudflared.log"
kill $CLOUDFLARED_PID 2>/dev/null
kill $DAEMON_PID 2>/dev/null
exit 1

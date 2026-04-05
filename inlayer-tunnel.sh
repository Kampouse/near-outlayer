#!/bin/bash
# inlayer daemon with automatic Cloudflare tunnel
# Usage: ./inlayer-tunnel.sh [--daemon]

set -e

# Find inlayer binary
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

# Check if --daemon flag
DAEMON_FLAG=""
if [ "$1" = "--daemon" ]; then
    DAEMON_FLAG="--daemon"
fi

echo "🚀 Starting inlayer daemon with Cloudflare tunnel..."
echo ""

# Start inlayer daemon
echo "1️⃣  Starting inlayer daemon..."
$INLAYER_BIN daemon --foreground --dashboard 127.0.0.1:8082 $DAEMON_FLAG > /tmp/inlayer-daemon.log 2>&1 &
DAEMON_PID=$!
echo "   ✅ Daemon PID: $DAEMON_PID"

# Wait for daemon to start
sleep 3

# Start Cloudflare tunnel
echo "2️⃣  Creating Cloudflare tunnel..."
cloudflared tunnel --url http://localhost:8082 > /tmp/cloudflared-tunnel.log 2>&1 &
TUNNEL_PID=$!
echo "   ✅ Tunnel PID: $TUNNEL_PID"

# Wait for tunnel URL
echo "   ⏳ Waiting for tunnel URL..."
for i in {1..15}; do
    TUNNEL_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' /tmp/cloudflared-tunnel.log 2>/dev/null | head -1)
    if [ -n "$TUNNEL_URL" ]; then
        echo ""
        echo "🎉 TUNNEL IS LIVE!"
        echo ""
        echo "📍 Public URL: $TUNNEL_URL"
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

        # Save tunnel info
        cat > /tmp/inlayer-tunnel-info.txt <<EOF
Inlayer Daemon + Cloudflare Tunnel
=====================================

📍 Public URL: $TUNNEL_URL

📡 Endpoints:
  Status:    $TUNNEL_URL/api/status
  Execute:   $TUNNEL_URL/call/<account>/<project>
  History:   $TUNNEL_URL/api/history
  Stream:    $TUNNEL_URL/api/stream
  Storage:   $TUNNEL_URL/api/storage
  Contract:  $TUNNEL_URL/api/contract

🔧 PIDs:
  Daemon:    $DAEMON_PID
  Tunnel:    $TUNNEL_PID

📝 Logs:
  Daemon:    /tmp/inlayer-daemon.log
  Tunnel:    /tmp/cloudflared-tunnel.log

🛑 Stop:
  kill $DAEMON_PID $TUNNEL_PID
EOF

        echo "💾 Info saved to: /tmp/inlayer-tunnel-info.txt"
        echo ""
        echo "✨ Press Ctrl+C to stop both daemon and tunnel"

        # Handle Ctrl+C
        cleanup() {
            echo ""
            echo "🛑 Stopping..."
            kill $TUNNEL_PID 2>/dev/null || true
            kill $DAEMON_PID 2>/dev/null || true
            echo "✅ Stopped"
            exit 0
        }
        trap cleanup INT TERM

        # Keep running
        wait
    fi
    sleep 1
    echo -n "."
done

echo ""
echo "❌ Timeout waiting for tunnel"
echo "   Check logs: cat /tmp/cloudflared-tunnel.log"
kill $TUNNEL_PID 2>/dev/null
kill $DAEMON_PID 2>/dev/null
exit 1

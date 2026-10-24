#!/bin/bash
# One-click chat mainline demo for the web-agent project.
# First-time setup only: chmod +x run-chat.command
# After that, double-clicking this file starts the Mock Gateway
# on localhost:3001, runs the Rust chat-cli demo (history /
# stream / apply, then the same turn with --cancel), stops the
# server, and leaves the window open so you can read the result.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Starting mock-gateway on http://127.0.0.1:3001 ..."
cargo run -q -p mock-gateway > /tmp/mock-gateway-chat.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
echo ""
echo "=== Normal turn ==="
cargo run -q -p agent-desktop --bin chat-cli -- --turn-id turn-demo --text "你好，我想先跑通聊天主线。"
echo ""
echo "=== Cancelled turn (apply after cancel must keep nothing) ==="
cargo run -q -p agent-desktop --bin chat-cli -- --stream-id stream-cancel-demo --turn-id turn-cancel-demo --text "这条会被取消。" --cancel
echo ""
echo "Done. The mock server has been stopped."
echo "Press Enter to close this window."
read -r _

#!/bin/bash
# One-click file mainline demo for the web-agent project.
# First-time setup only: chmod +x run-files.command
# After that, double-clicking this file starts the Mock Gateway
# on localhost:3001 (file root ./mock-files), runs the Rust
# file-cli demo (list, write with approval, chunked read, write
# without approval which must be refused, delete with approval,
# list again), stops the server, and leaves the window open.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Starting mock-gateway on http://127.0.0.1:3001 (file root ./mock-files) ..."
cargo run -q -p mock-gateway > /tmp/mock-gateway-files.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
CLI="cargo run -q -p agent-desktop --bin file-cli"
echo ""
echo "=== list (before) ==="
$CLI list
echo ""
echo "=== write with approval ==="
$CLI write --path demo.txt --text "hello world" --approve
echo ""
echo "=== chunked read ==="
$CLI read --path demo.txt --chunked --len 5
echo ""
echo "=== write WITHOUT approval (must be refused) ==="
$CLI write --path demo.txt --text "should not land" || echo "(refused as expected)"
echo ""
echo "=== delete with approval ==="
$CLI delete --path demo.txt --approve
echo ""
echo "=== list (after) ==="
$CLI list
echo ""
echo "Done. The mock server has been stopped."
echo "Press Enter to close this window."
read -r _

#!/bin/bash
# One-click agent invocation demo for the web-agent project.
# First-time setup only: chmod +x run-agent.command
# After that, double-clicking this file starts the Mock Gateway
# on localhost:3001 (file root ./mock-files), runs the Rust
# agent-cli demo (echo without approval, fs.write without
# approval which must stop at a pending record, fs.write with
# approval which executes, fs.read back, fs.write with a
# rejected decision which must be refused server-side),
# stops the server, and leaves the window open.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Starting mock-gateway on http://127.0.0.1:3001 (file root ./mock-files) ..."
cargo run -q -p mock-gateway > /tmp/mock-gateway-agent.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
CLI="cargo run -q -p agent-desktop --bin agent-cli"
echo ""
echo "=== echo (no approval needed) ==="
$CLI call --tool echo --arg text=hi

echo ""
echo "=== fs.write WITHOUT approval (must stop at a pending record) ==="
$CLI call --tool fs.write --arg path=agent-demo.txt --arg text="from agent" || echo "(stopped at pending approval, as expected)"

echo ""
echo "=== fs.write WITH approval ==="
$CLI call --tool fs.write --arg path=agent-demo.txt --arg text="from agent" --approve

echo ""
echo "=== fs.read back ==="
$CLI call --tool fs.read --arg path=agent-demo.txt

echo ""
echo "=== fs.write WITH rejected decision (must be refused server-side) ==="
$CLI call --tool fs.write --arg path=agent-demo.txt --arg text="should not land" --reject || echo "(refused server-side, as expected)"

echo ""
echo "=== fs.read again (content must be unchanged) ==="
$CLI call --tool fs.read --arg path=agent-demo.txt

echo ""
echo "Done. The mock server has been stopped."
echo "Press Enter to close this window."
read -r _

#!/bin/bash
# One-click Mock connector demo for the web-agent project.
# First-time setup only: chmod +x run-connectors.command
# After that, double-clicking this file starts the Mock Gateway
# on localhost:3001 (file root ./mock-files), runs the Rust
# connector-cli demo (fake notes/mail lists, note create
# without approval which must stop at pending, note create
# with approval, mail send outside mock.local which must be
# refused by egress, mail send inside mock.local with
# approval, credential store redacted display), stops the
# server, and leaves the window open.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Starting mock-gateway on http://127.0.0.1:3001 (file root ./mock-files) ..."
cargo run -q -p mock-gateway > /tmp/mock-gateway-connectors.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
CLI="cargo run -q -p agent-desktop --bin connector-cli"
echo ""
echo "=== notes list (fake seed data) ==="
$CLI notes list
echo ""
echo "=== mail list (fake seed data) ==="
$CLI mail list
echo ""
echo "=== notes create WITHOUT approval (must stop at pending) ==="
$CLI notes create --title "Mock created" --body "created in this run" || echo "(stopped at pending approval, as expected)"
echo ""
echo "=== notes create WITH approval ==="
$CLI notes create --title "Mock created" --body "created in this run" --approve
echo ""
echo "=== notes list again (must show the new note) ==="
$CLI notes list
echo ""
echo "=== mail send OUTSIDE mock.local (must be refused by egress) ==="
$CLI mail send --to a@example.com --subject "Hi" --body "hello" --approve || echo "(refused by egress, as expected)"
echo ""
echo "=== mail send INSIDE mock.local WITH approval ==="
$CLI mail send --to a@mock.local --subject "Mock hello" --body "sent in this run" --approve
echo ""
echo "=== credential store (memory only: a fresh process has no stored credential) ==="
$CLI cred store --handle mock --secret mock-token-1234
$CLI cred show --handle mock
echo ""
echo "Done. The mock server has been stopped."
echo "Press Enter to close this window."
read -r _

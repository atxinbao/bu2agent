#!/bin/bash
# One-click Tauri 2 shell demo for the web-agent project.
# First-time setup only: chmod +x run-shell.command
# First run also needs the Tauri CLI once: cargo install tauri-cli --locked --version '^2'
# After that, double-clicking starts the Mock Gateway on
# localhost:3001, the Vite dev server on localhost:5173, then
# opens the Tauri window. Close with Ctrl-C; the trap below
# stops both background servers.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
if ! cargo tauri --version > /dev/null 2>&1; then
  echo "Tauri CLI is not installed. Run this once, then re-run this script:"
  echo "  cargo install tauri-cli --locked --version '^2'"
  echo "Press Enter to close this window."
  read -r _
  exit 1
fi
if [ ! -d apps/web/node_modules ]; then
  echo "Installing frontend dependencies (first run only) ..."
  (cd apps/web && npm install) || exit 1
fi
echo "Starting mock-gateway on http://127.0.0.1:3001 ..."
cargo run -q -p mock-gateway > /tmp/mock-gateway-shell.log 2>&1 &
SERVER_PID=$!
echo "Starting Vite dev server on http://localhost:5173 ..."
(cd apps/web && npm run dev > /tmp/vite-shell.log 2>&1) &
VITE_PID=$!
trap 'kill $SERVER_PID $VITE_PID 2>/dev/null' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
# Fail loudly instead of opening a window that can only report
# "gateway unreachable": if the gateway never became healthy,
# the log below is the diagnosis, not the window.
if ! curl -s -f http://127.0.0.1:3001/health > /dev/null 2>&1; then
  echo "Mock gateway did not become healthy on http://127.0.0.1:3001."
  echo "See /tmp/mock-gateway-shell.log. Not opening the window."
  exit 1
fi
for i in $(seq 1 20); do
  if curl -s http://localhost:5173/ > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -s -f http://localhost:5173/ > /dev/null 2>&1; then
  echo "Vite dev server did not become healthy on http://localhost:5173."
  echo "See /tmp/vite-shell.log. Not opening the window."
  exit 1
fi
echo ""
echo "Opening the Tauri window (close it with Ctrl-C here) ..."
echo "Expected in the window: gateway ok, shell bridge ok (ping=pong),"
echo "seed history messages, and sending a message appends a reply."
(cd apps/shell/src-tauri && cargo tauri dev)
echo ""
echo "Done. Both background servers have been stopped."
echo "Press Enter to close this window."
read -r _

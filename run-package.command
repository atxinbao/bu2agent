#!/bin/bash
# Packaging smoke check for the web-agent project (Phase 5).
# First-time setup only: chmod +x run-package.command
# After that, double-clicking this file runs the release build
# and lists the release binaries (four demo CLIs + the Mock
# Gateway). It does not build a Tauri shell or an installer:
# those are still pending (see packaging/README.md).
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Building release binaries ..."
cargo build --release -p agent-desktop -p mock-gateway
BUILD_STATUS=$?
if [ $BUILD_STATUS -ne 0 ]; then
  echo "FAIL: release build failed."
  echo "Press Enter to close this window."
  read -r _
  exit 1
fi
echo ""
echo "=== release binaries ==="
ls -la target/release/chat-cli target/release/file-cli target/release/agent-cli target/release/connector-cli target/release/mock-gateway

echo ""
echo "=== CLI smoke (no-args run must exit, not hang) ==="
for bin in chat-cli file-cli agent-cli connector-cli; do
  ./target/release/$bin > /dev/null 2>&1
  echo "$bin exited with code $? (no-args run; real runs need the Mock Gateway on localhost:3001)"
done
echo ""
echo "Done. Release binaries are in target/release/."
echo "Press Enter to close this window."
read -r _

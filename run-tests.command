#!/bin/bash
# One-click test runner for the web-agent project (hybrid loop, Mac side).
# First-time setup only: chmod +x run-tests.command
# After that, double-clicking this file runs the full workspace test suite
# in Terminal and leaves the window open so you can read the result.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
echo "Running: cargo test (workspace: agent-desktop + mock-gateway)"
echo "Project: $(pwd)"
cargo test
TEST_STATUS=$?
echo ""
if [ "$TEST_STATUS" -eq 0 ]; then
  echo "PASS: all tests passed."
else
  echo "FAIL: cargo test exited with status $TEST_STATUS. Copy the output above and send it to 小宝."
fi
echo "Press Enter to close this window."
read -r _

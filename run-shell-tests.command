#!/bin/bash
# One-click shell command tests for the web-agent project (no window).
# First-time setup only: chmod +x run-shell-tests.command
# Double-clicking starts a throwaway Mock Gateway on
# localhost:3001 with a fresh temp file root, runs the shell
# crate's unit tests (they call the 23 Tauri commands in
# tool_commands.rs directly against that gateway), then stops
# the server and deletes the temp root. No Tauri window opens.
# If port 3001 is already healthy (run-shell is up), this
# script refuses to run: the commands hardcode 127.0.0.1:3001,
# and sharing a live session's server would pollute its state.
cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"
if curl -s -f http://127.0.0.1:3001/health > /dev/null 2>&1; then
  echo "Port 3001 is already serving a Mock Gateway (is run-shell still up?)."
  echo "Stop it first, then re-run this script, so the tests get a fresh server."
  echo "Press Enter to close this window."
  read -r _
  exit 1
fi
TEST_ROOT=$(mktemp -d /tmp/web-agent-shell-tests.XXXXXX)
# 审批日志与业务文件分开，避免日志污染文件列表断言。
FILES_ROOT="$TEST_ROOT/files"
mkdir -p "$FILES_ROOT"
echo "Starting throwaway mock-gateway on http://127.0.0.1:3001 ..."
echo "File root: $FILES_ROOT (deleted when this script exits)"
# 回归服务不读取 env 或 Keychain 中的真实模型凭据。
MOCK_FILES_ROOT="$FILES_ROOT" MOCK_APPROVALS_PATH="$TEST_ROOT/approvals.jsonl" MOCK_TASKS_PATH="$TEST_ROOT/tasks.jsonl" MOCK_DEVICE_ACTIONS_PATH="$TEST_ROOT/actions.jsonl" MOCK_LOOPS_PATH="$TEST_ROOT/loops.jsonl" cargo run -q -p mock-gateway -- --mock-chat > /tmp/mock-gateway-shell-tests.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null; rm -rf "$TEST_ROOT"' EXIT
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -s -f http://127.0.0.1:3001/health > /dev/null 2>&1; then
  echo "Mock gateway did not become healthy on http://127.0.0.1:3001."
  echo "See /tmp/mock-gateway-shell-tests.log."
  exit 1
fi
echo "Running: cargo test in apps/shell/src-tauri (single-threaded: the tests share one server)"
(cd apps/shell/src-tauri && cargo test -- --test-threads=1)
TEST_STATUS=$?
echo ""
if [ "$TEST_STATUS" -eq 0 ]; then
  echo "PASS: all shell command tests passed."
else
  echo "FAIL: shell command tests exited with status $TEST_STATUS. Copy the output above and send it to 小宝."
fi
echo "Press Enter to close this window."
read -r _

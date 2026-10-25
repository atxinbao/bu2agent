#!/bin/bash
# EventKit read-only verification for the bu2agent gateway (no window).
#
# Builds the gateway, runs the real binary with the EventKit
# backend, and prints ONLY permission state plus the reminder
# COUNT. Reminder titles and contents are never printed: the
# count step first requires HTTP 200 and a JSON array, then
# prints only the array length; any other response reports
# failure without a count (an error object must never be
# counted as one reminder).
#
# What this verifies (and what it does not):
# - the eventkit crate compiles and the real binary can read
#   its own authorization status;
# - whether a system prompt appears, and what status results;
# - 父进程链仅是启动方式旁证，不独立证明 TCC 负责进程归属。
# 本脚本只调用授权与读取端点，不创建、修改或删除提醒。
# 当前 EventKit 后端已有真实写路径；本脚本不调用该路径。
# It does NOT prove a bundled gateway would hold its own
# permission: a bare binary run from Terminal inherits its
# responsible process's permission (see the crate docs).
# That bundling question is a follow-up decision, not a
# claim this script can settle.
#
# Usage: double-click run-eventkit-check.command. When the
# output is complete, paste the status lines (not the whole
# log) back to the assistant. Press Enter to close the window.

cd "$(dirname "$0")" || exit 1
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"

if curl -s -f http://127.0.0.1:3001/health > /dev/null 2>&1; then
  echo "Port 3001 is already serving a Mock Gateway (is run-shell still up?)."
  echo "Stop it first, then re-run this script."
  echo "Press Enter to close this window."
  read -r _
  exit 1
fi

TEST_ROOT=$(mktemp -d /tmp/bu2agent-eventkit-check.XXXXXX)
FILES_ROOT="$TEST_ROOT/files"
mkdir -p "$FILES_ROOT"
# Install cleanup before the build: a failed build used to
# leave the temp dir behind because the trap came later.
SERVER_PID=""
trap '[ -n "$SERVER_PID" ] && kill $SERVER_PID 2>/dev/null; rm -rf "$TEST_ROOT"' EXIT

echo "Building mock-gateway (EventKit backend is macOS-only) ..."
cargo build -q -p mock-gateway || exit 1

echo "Starting gateway with the EventKit backend ..."
# --mock-chat keeps the gateway off the real model Keychain:
# this check needs no chat, and Keychain prompts would only
# confuse the authorization observation below.
MOCK_FILES_ROOT="$FILES_ROOT" MOCK_DEVICE_BACKEND=eventkit ./target/debug/mock-gateway -- --mock-chat > /tmp/mock-gateway-eventkit.log 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 20); do
  if curl -s http://127.0.0.1:3001/health > /dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -s -f http://127.0.0.1:3001/health > /dev/null 2>&1; then
  echo "Mock gateway did not become healthy. See /tmp/mock-gateway-eventkit.log."
  exit 1
fi

echo "Gateway binary: $PWD/target/debug/mock-gateway (unsigned, no bundle) PID $SERVER_PID"
PARENT_PID=$(ps -o ppid= -p $SERVER_PID | tr -d ' ')
echo "Gateway parent PID: $PARENT_PID ($(ps -o comm= -p $PARENT_PID | tr -d ' '))"
GRANDPARENT_PID=$(ps -o ppid= -p $PARENT_PID | tr -d ' ')
echo "Gateway grandparent PID: $GRANDPARENT_PID ($(ps -o comm= -p $GRANDPARENT_PID | tr -d ' '))"
echo ""
echo "Authorization status before requesting (read-only):"
curl -s http://127.0.0.1:3001/devices/authorization
echo ""
echo "Requesting system authorization now: a macOS prompt may appear."
echo "If it appears, note what it says and what you click (Allow or Don't Allow)."
echo "If nothing appears, that itself is a finding (see the checklist)."
curl -s -X POST http://127.0.0.1:3001/devices/authorization/request
echo ""
echo "Authorization status after requesting:"
curl -s http://127.0.0.1:3001/devices/authorization
echo ""
echo "Reminder count (titles are never printed):"
LIST_RESPONSE=$(curl -s -w "\n%{http_code}" http://127.0.0.1:3001/devices/reminders/list)
LIST_STATUS=$(printf '%s' "$LIST_RESPONSE" | tail -n 1)
LIST_BODY=$(printf '%s' "$LIST_RESPONSE" | sed '$d')
if [ "$LIST_STATUS" != "200" ]; then
  echo "reminder list request failed (HTTP $LIST_STATUS); count not reported."
else
  printf '%s' "$LIST_BODY" | python3 -c "
import json, sys
data = json.load(sys.stdin)
if isinstance(data, list):
    print('reminder_count:', len(data))
else:
    print('unexpected list response (not an array); count not reported.')
"
fi
echo ""
echo "Done. The server stops when this window closes."
echo "Press Enter to close this window."
read -r _

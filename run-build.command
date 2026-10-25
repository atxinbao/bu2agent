#!/bin/bash
# bu2agent 首个安装包：先构建前端，再打 Tauri release 包。
# 注意：tauri.conf 未配 beforeBuildCommand，所以这里先跑 npm run build，
# 否则打包会用旧的 web/dist（可能还是聊天线老界面）。
set -e
cd "$(dirname "$0")"
export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"

if ! command -v cargo >/dev/null 2>&1; then
  echo "FAIL: 未找到 cargo（应为 ~/.local/bin/cargo）"
  exit 1
fi
if ! cargo tauri --version >/dev/null 2>&1; then
  echo "FAIL: 未找到 tauri CLI，请先装：cargo install tauri-cli --locked --version '^2'"
  exit 1
fi
if ! command -v npm >/dev/null 2>&1; then
  echo "FAIL: 未找到 npm（应为 /opt/homebrew/bin/npm）"
  exit 1
fi

echo "--- 构建前端 ---"
cd apps/web
if [ ! -d node_modules ]; then
  npm install
fi
npm run build
cd ../shell/src-tauri

echo "--- 打包 Tauri release（首次构建较久，请耐心等）---"
cargo tauri build

echo "--- 打包产物 ---"
ls -la target/release/bundle/macos/
echo "PASS: 安装包已生成"

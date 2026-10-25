#!/bin/bash
# bu2agent 图标集一键生成：从 1024x1024 源图切出整套图标。
# 前置：把确认的图标 PNG 存为 apps/shell/src-tauri/icons/icon-source.png
set -e
cd "$(dirname "$0")"
export PATH="$HOME/.local/bin:$PATH"

SRC="apps/shell/src-tauri/icons/icon-source.png"
if [ ! -f "$SRC" ]; then
  echo "FAIL: 未找到源图 $SRC"
  echo "请先把确认的图标 PNG 存到该路径，再重跑本脚本。"
  exit 1
fi

if command -v sips >/dev/null 2>&1; then
  sips -g pixelWidth -g pixelHeight "$SRC"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "FAIL: 未找到 cargo（应为 ~/.local/bin/cargo）"
  exit 1
fi
if ! cargo tauri --version >/dev/null 2>&1; then
  echo "FAIL: 未找到 tauri CLI，请先装：cargo install tauri-cli --locked --version '^2'"
  exit 1
fi

cd apps/shell/src-tauri
cargo tauri icon icons/icon-source.png

echo "--- icons/ 生成结果 ---"
ls -la icons/
echo "PASS: 图标集已生成"

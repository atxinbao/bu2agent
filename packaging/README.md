# Packaging (Phase 5)

## What ships today

The workspace builds two crates:

- `agent-desktop`: the Rust client shell library plus four demo CLIs
  (`chat-cli`, `file-cli`, `agent-cli`, `connector-cli`). These are
  data-flow demos, not a windowed client.
- `mock-gateway`: the localhost Mock Gateway binary (default
  `127.0.0.1:3001`, override with `MOCK_GATEWAY_ADDR`).

Release build: `cargo build --release`. Binaries land in
`target/release/` (`chat-cli`, `file-cli`, `agent-cli`,
`connector-cli`, `mock-gateway`). `run-package.command` runs this
build and lists the release binaries as a packaging smoke check.

## Three-system gate

`.github/workflows/ci.yml` runs `cargo test --workspace` and
`cargo clippy --all-targets` on macOS, Linux, and Windows, plus
the TypeScript strict check, on every push and pull request.
A change counts as verified only when it passes on all three.
The workflow file is checked into the project but has not been
run on GitHub yet: the project is not in GitHub, so CI is a
standing gate, not yet an observed green run. Saying otherwise
would be inventing a result.

## Explicitly not shipped yet

- No Tauri window / WebView shell, no frontend rendering.
- No signed installer, no notarization, no auto-update.
- No real gateway transport (Noise), no real keychain, no real
  connector OAuth. The Mock Gateway is localhost-only by design.

Those are the next steps after Phase 5, in the order the plan
sets: Tauri shell + frontend rendering first, then real
transport and real credentials.

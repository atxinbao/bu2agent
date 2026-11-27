//! Mock Gateway binary: serves the pure-logic state machine in
//! `mock_gateway` over HTTP via `server.rs`.
//!
//! Localhost only by default. Override with MOCK_GATEWAY_ADDR,
//! e.g. `MOCK_GATEWAY_ADDR=127.0.0.1:3001 cargo run -p mock-gateway`.
//! Approval records persist to MOCK_APPROVALS_PATH (JSONL) when
//! set; when unset, records stay memory-only (see server.rs).
//! Chat uses DeepSeek (model `deepseek-flash`) when a key is
//! configured: env DEEPSEEK_API_KEY, else the OS keychain
//! handle `deepseek`; without a key, chat keeps the Mock echo.
//! Ctrl-C stops the server gracefully (see server::serve).

#[tokio::main]
async fn main() {
    // 壳回归专用参数跳过凭据读取，正常启动仍按 env、Keychain 顺序取 key。
    let config = if std::env::args().any(|arg| arg == "--mock-chat") {
        mock_gateway::deepseek::DeepSeekConfig::disabled()
    } else {
        mock_gateway::deepseek::DeepSeekConfig::from_env_and_keychain()
    };
    println!("chat backend: {}", if config.api_key.is_some() { "deepseek" } else { "mock echo (no key configured or test mode)" });
    let state = mock_gateway::server::AppState::from_deepseek_config(config);
    // P0-E fail-closed startup: if the recovery scan could not
    // establish this process's worker identity, the gateway
    // must not open with a half-scanned assignment table.
    if state.sqlite_recovery_failed() {
        eprintln!("mock-gateway refusing to start: P0-E recovery scan failed (see above); fix the database and restart");
        std::process::exit(1);
    }
    let addr =
        std::env::var("MOCK_GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind mock gateway listener");
    println!("mock-gateway listening on http://{addr}");
    // AO-3 (AO3-J1): the gateway writes its own lockfile
    // on startup, whoever started it, so the shell can
    // attach to a launchd/terminal gateway. Only when no
    // live lock exists — never clobber a live gateway's
    // lock. Best-effort: a failed write must not stop
    // the gateway.
    let lock_pid = std::process::id();
    let lock_port = listener.local_addr().map(|a| a.port()).unwrap_or(3001);
    if mock_gateway::gateway_lock::live_lock().is_none() {
        let lock = mock_gateway::gateway_lock::GatewayLock {
            pid: lock_pid,
            port: lock_port,
            started_at_ms: mock_gateway::gateway_lock::now_ms(),
        };
        if let Err(detail) = mock_gateway::gateway_lock::write_lock(&lock) {
            eprintln!("warning: gateway lockfile write failed: {detail}");
        }
    }
    let served = mock_gateway::server::serve(listener, state).await;
    // Graceful stop (Ctrl-C): remove the lock, but only
    // if it still names this process.
    mock_gateway::gateway_lock::remove_lock_if_ours(lock_pid);
    served.expect("serve mock gateway");
}

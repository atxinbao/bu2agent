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
    let state = mock_gateway::server::new_state()
        .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(config));
    let addr =
        std::env::var("MOCK_GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind mock gateway listener");
    println!("mock-gateway listening on http://{addr}");
    mock_gateway::server::serve(listener, state)
        .await
        .expect("serve mock gateway");
}

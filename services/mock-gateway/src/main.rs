//! Mock Gateway binary: serves the pure-logic state machine in
//! `mock_gateway` over HTTP via `server.rs`.
//!
//! Localhost only by default. Override with MOCK_GATEWAY_ADDR,
//! e.g. `MOCK_GATEWAY_ADDR=127.0.0.1:3001 cargo run -p mock-gateway`.
//! Ctrl-C stops the server gracefully (see server::serve).

#[tokio::main]
async fn main() {
    let addr =
        std::env::var("MOCK_GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind mock gateway listener");
    println!("mock-gateway listening on http://{addr}");
    mock_gateway::server::serve(listener, mock_gateway::server::new_state())
        .await
        .expect("serve mock gateway");
}

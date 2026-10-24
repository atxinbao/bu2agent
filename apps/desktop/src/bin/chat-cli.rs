//! Chat mainline demo CLI: Rust shell -> Mock Gateway over HTTP.
//!
//! This is the runnable end of the Phase 1 chat mainline. It is
//! not a UI and not a Tauri shell yet: it proves history, stream
//! start, stream apply (dedup) and cancel through the real Axum
//! transport, using the same `ChatClient` the shell will use.
//!
//! Usage (with the Mock Gateway running on localhost:3001):
//! ```sh
//! cargo run -p agent-desktop --bin chat-cli -- --turn-id turn-demo --text "hello"
//! cargo run -p agent-desktop --bin chat-cli -- --turn-id turn-demo --text "hello" --cancel
//! ```
//! Options: --base-url (default http://127.0.0.1:3001),
//! --stream-id (default stream-demo), --turn-id, --text, --cancel.

use agent_desktop::chat_client::{assemble_text, ChatClient};

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base_url = arg_value(&args, "--base-url", "http://127.0.0.1:3001");
    let stream_id = arg_value(&args, "--stream-id", "stream-demo");
    let turn_id = arg_value(&args, "--turn-id", "turn-demo");
    let text = arg_value(&args, "--text", "hello");
    let cancel = args.iter().any(|a| a == "--cancel");

    if let Err(err) = run(&base_url, &stream_id, &turn_id, &text, cancel) {
        eprintln!("chat-cli failed: {} ({:?})", err.redacted_detail, err.code);
        std::process::exit(1);
    }
}

fn run(
    base_url: &str,
    stream_id: &str,
    turn_id: &str,
    text: &str,
    cancel: bool,
) -> agent_desktop::Result<()> {
    let client = ChatClient::new(base_url)?;
    println!("health: {}", client.health()?);

    println!("history:");
    for message in client.history()? {
        println!("  [{}] {}: {}", message.turn_id, message.role, message.text);
    }

    let records = client.start_stream(stream_id, turn_id, text)?;
    println!(
        "started stream {stream_id} turn {turn_id}: {} records, sequences {:?}",
        records.len(),
        records.iter().map(|r| r.sequence).collect::<Vec<_>>()
    );

    if cancel {
        let ok = client.cancel_stream(stream_id)?;
        println!("cancelled stream {stream_id}: {ok}");
    }
    let applied = client.apply_stream(stream_id, &records)?;
    println!(
        "applied: {} kept, last_applied_sequence={}",
        applied.kept.len(),
        applied.last_applied_sequence
    );
    if cancel {
        println!("assistant text after cancel: (empty, as expected)");
    } else {
        println!("assistant text: {}", assemble_text(&applied.kept));
    }
    Ok(())
}

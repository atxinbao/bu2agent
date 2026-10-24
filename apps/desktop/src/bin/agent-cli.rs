//! Agent invocation demo CLI: Rust shell -> Mock Gateway, with
//! the approval loop closed end to end.
//!
//! Usage (with the Mock Gateway running on localhost:3001):
//! ```sh
//! cargo run -p agent-desktop --bin agent-cli -- call --tool echo --arg text=hi
//! cargo run -p agent-desktop --bin agent-cli -- call --tool fs.write --arg path=demo.txt --arg text="hello" --approve
//! cargo run -p agent-desktop --bin agent-cli -- call --tool fs.read --arg path=demo.txt
//! cargo run -p agent-desktop --bin agent-cli -- call --tool fs.write --arg path=demo.txt --arg text="x" --reject
//! ```
//! Without --approve or --reject, an approval tool stops at a
//! pending record and prints its tool_call_id; nothing executes.
//! With --reject, the decision is recorded as rejected and the
//! call is then refused server-side, proving the server gate.

use std::collections::HashMap;

use agent_desktop::agent_client::{AgentClient, AgentError, ToolDecision};

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).cloned().unwrap_or_else(|| "call".to_string());
    if command != "call" {
        eprintln!("unknown agent-cli command: {command} (call)");
        std::process::exit(2);
    }
    let base_url = arg_value(&args, "--base-url", "http://127.0.0.1:3001");
    let tool = arg_value(&args, "--tool", "");
    let mut tool_args: HashMap<String, String> = HashMap::new();
    for pair in args.windows(2).filter(|w| w[0] == "--arg").map(|w| w[1].clone()) {
        if let Some((key, value)) = pair.split_once('=') {
            tool_args.insert(key.to_string(), value.to_string());
        }
    }
    let decision = if args.iter().any(|a| a == "--approve") {
        ToolDecision::Approve
    } else if args.iter().any(|a| a == "--reject") {
        ToolDecision::Reject
    } else {
        ToolDecision::Ask
    };
    let client = match AgentClient::new(&base_url) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("agent-cli failed: {} ({:?})", err.redacted_detail, err.code);
            std::process::exit(1);
        }
    };
    match client.run_tool(&tool, &tool_args, decision) {
        Ok(result) => {
            println!("tool {} executed", result.tool);
            println!("{}", serde_json::to_string_pretty(&result.result).unwrap_or_default());
        }
        Err(AgentError::ApprovalPending { tool_call_id }) => {
            println!("approval required: tool_call_id={tool_call_id} (re-run with --approve or --reject)");
            std::process::exit(1);
        }
        Err(AgentError::Unified(err)) => {
            eprintln!("agent-cli failed: {} ({:?})", err.redacted_detail, err.code);
            std::process::exit(1);
        }
    }
}

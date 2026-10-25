//! Mock connector demo CLI: Rust shell -> Mock Gateway connectors,
//! plus the credential store (memory or OS keychain) and the egress policy.
//!
//! Usage (with the Mock Gateway running on localhost:3001):
//! ```sh
//! cargo run -p agent-desktop --bin connector-cli -- notes list
//! cargo run -p agent-desktop --bin connector-cli -- notes create --title "T" --body "B" --approve
//! cargo run -p agent-desktop --bin connector-cli -- mail list
//! cargo run -p agent-desktop --bin connector-cli -- mail send --to a@mock.local --subject "S" --body "B" --approve
//! cargo run -p agent-desktop --bin connector-cli -- cred store --handle mock --secret mock-token-1234
//! cargo run -p agent-desktop --bin connector-cli -- cred show --handle mock
//! cargo run -p agent-desktop --bin connector-cli -- cred store --keychain --handle mock --secret mock-token-1234
//! cargo run -p agent-desktop --bin connector-cli -- cred show --keychain --handle mock
//! cargo run -p agent-desktop --bin connector-cli -- cred remove --keychain --handle mock
//! ```
//! All connector data is fake (Mock only). The credential store
//! is memory-only by default; `--keychain` switches it to the OS
//! keychain, which survives across processes. `cred show` prints
//! only the redacted form, never the secret. Mail outside
//! `mock.local` is refused by the egress policy before any request
//! is sent.

use agent_desktop::agent_client::{AgentError, ToolDecision};
use agent_desktop::connector_client::ConnectorClient;
use agent_desktop::credential_store::{CredentialStore, KeychainCredentialStore, MemoryCredentialStore};

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn cred_store(use_keychain: bool) -> Box<dyn CredentialStore> {
    if use_keychain {
        Box::new(KeychainCredentialStore::new())
    } else {
        Box::new(MemoryCredentialStore::new())
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let group = args.get(1).cloned().unwrap_or_default();
    let command = args.get(2).cloned().unwrap_or_default();
    let base_url = arg_value(&args, "--base-url", "http://127.0.0.1:3001");
    let decision = if args.iter().any(|a| a == "--approve") {
        ToolDecision::Approve
    } else if args.iter().any(|a| a == "--reject") {
        ToolDecision::Reject
    } else {
        ToolDecision::Ask
    };
    let use_keychain = args.iter().any(|a| a == "--keychain");
    let code = match (group.as_str(), command.as_str()) {
        ("cred", "store") => {
            let store = cred_store(use_keychain);
            let handle = arg_value(&args, "--handle", "mock");
            let secret = arg_value(&args, "--secret", "");
            match store.store(&handle, &secret) {
                Ok(()) => {
                    if use_keychain {
                        println!("stored credential handle={handle} (keychain, persists across processes)");
                    } else {
                        println!("stored credential handle={handle} (memory only, secret never printed)");
                    }
                    0
                }
                Err(err) => {
                    eprintln!("connector-cli failed: {} ({:?})", err.redacted_detail, err.code);
                    1
                }
            }
        }
        ("cred", "show") => {
            let store = cred_store(use_keychain);
            let handle = arg_value(&args, "--handle", "mock");
            match store.redacted(&handle) {
                Ok(display) => {
                    println!("credential {handle}: {display}");
                    0
                }
                Err(_) => {
                    if use_keychain {
                        println!("credential {handle}: not stored in keychain");
                    } else {
                        println!("credential {handle}: not stored in this process (memory only)");
                    }
                    0
                }
            }
        }
        ("cred", "remove") => {
            let store = cred_store(use_keychain);
            let handle = arg_value(&args, "--handle", "mock");
            match store.remove(&handle) {
                Ok(()) => {
                    println!("removed credential handle={handle}");
                    0
                }
                Err(err) => {
                    eprintln!("connector-cli failed: {} ({:?})", err.redacted_detail, err.code);
                    1
                }
            }
        }
        ("notes", "list") | ("mail", "list") | ("notes", "create") | ("mail", "send") => {
            run_connector(&group, &command, &base_url, &args, decision)
        }
        _ => {
            eprintln!("unknown connector-cli command: {group} {command}");
            eprintln!("usage: notes list | notes create | mail list | mail send | cred store | cred show | cred remove [--keychain]");
            2
        }
    };
    std::process::exit(code);
}

fn run_connector(
    group: &str,
    command: &str,
    base_url: &str,
    args: &[String],
    decision: ToolDecision,
) -> i32 {
    let client = match ConnectorClient::new(base_url) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("connector-cli failed: {} ({:?})", err.redacted_detail, err.code);
            return 1;
        }
    };
    let title = arg_value(args, "--title", "");
    let body = arg_value(args, "--body", "");
    let to = arg_value(args, "--to", "");
    let subject = arg_value(args, "--subject", "");
    let result = match (group, command) {
        ("notes", "list") => client.notes_list().map(|notes| {
            for note in notes {
                println!("  {}: {} -- {}", note.id, note.title, note.body);
            }
        }),
        ("mail", "list") => client.mail_list().map(|messages| {
            for message in messages {
                println!("  {}: {} -> {} -- {}", message.id, message.from, message.to, message.subject);
            }
        }),
        ("notes", "create") => client.notes_create(&title, &body, decision).map(|note| {
            println!("created note {}: {}", note.id, note.title);
        }),
        ("mail", "send") => client.mail_send(&to, &subject, &body, decision).map(|message| {
            println!("sent mail {}: {} -> {}", message.id, message.from, message.to);
        }),
        _ => unreachable!(),
    };
    match result {
        Ok(()) => 0,
        Err(AgentError::ApprovalPending { tool_call_id }) => {
            println!("approval required: tool_call_id={tool_call_id} (re-run with --approve or --reject)");
            1
        }
        Err(AgentError::Unified(err)) => {
            eprintln!("connector-cli failed: {} ({:?})", err.redacted_detail, err.code);
            1
        }
    }
}

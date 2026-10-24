//! File mainline demo CLI: Rust shell -> Mock Gateway file endpoints,
//! or, with --local-root, the local scoped file client.
//!
//! Usage (with the Mock Gateway running on localhost:3001):
//! ```sh
//! cargo run -p agent-desktop --bin file-cli -- list
//! cargo run -p agent-desktop --bin file-cli -- write --path demo.txt --text "hello" --approve
//! cargo run -p agent-desktop --bin file-cli -- read --path demo.txt
//! cargo run -p agent-desktop --bin file-cli -- read --path demo.txt --chunked
//! cargo run -p agent-desktop --bin file-cli -- delete --path demo.txt --approve
//! ```
//! Writes and deletes without --approve are refused by the client
//! before any request is sent or any disk is touched. The gateway
//! path is scoped to the server's root; the local path is scoped
//! to --local-root.

use agent_desktop::file_client::{ChunkedRead, FileClient, ScopedFileClient};
use agent_desktop::file_gateway_client::FileGatewayClient;

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

/// Parsed CLI flags, bundled so the run functions take one
/// argument instead of a flat nine-argument list (Phase 5).
struct CliArgs<'a> {
    command: &'a str,
    base_url: &'a str,
    path: &'a str,
    text: &'a str,
    offset: u64,
    len: u64,
    approved: bool,
    chunked: bool,
    local_root: &'a str,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).cloned().unwrap_or_else(|| "list".to_string());
    let base_url = arg_value(&args, "--base-url", "http://127.0.0.1:3001");
    let path = arg_value(&args, "--path", "");
    let text = arg_value(&args, "--text", "");
    let offset: u64 = arg_value(&args, "--offset", "0").parse().unwrap_or(0);
    let len: u64 = arg_value(&args, "--len", "4096").parse().unwrap_or(4096);
    let approved = args.iter().any(|a| a == "--approve");
    let chunked = args.iter().any(|a| a == "--chunked");
    let local_root = arg_value(&args, "--local-root", "");
    let parsed = CliArgs {
        command: &command,
        base_url: &base_url,
        path: &path,
        text: &text,
        offset,
        len,
        approved,
        chunked,
        local_root: &local_root,
    };
    if let Err(err) = run(&parsed) {
        eprintln!("file-cli failed: {} ({:?})", err.redacted_detail, err.code);
        std::process::exit(1);
    }
}

fn run(args: &CliArgs) -> agent_desktop::Result<()> {
    if !args.local_root.is_empty() {
        return run_local(args);
    }
    let client = FileGatewayClient::new(args.base_url)?;
    match args.command {
        "list" => {
            for entry in client.list(args.path)? {
                println!(
                    "  {} {} ({} bytes)",
                    if entry.is_dir { "dir " } else { "file" },
                    entry.path,
                    entry.size_bytes
                );
            }
            Ok(())
        }
        "read" => {
            if args.chunked {
                let data = client.read_all_chunked(args.path, args.len, 1_048_576)?;
                println!("{}", String::from_utf8_lossy(&data));
            } else {
                let chunk = client.read_chunk(args.path, args.offset, args.len)?;
                println!("{}", String::from_utf8_lossy(&chunk.data));
                println!("eof={} total_size={} bytes_read={}", chunk.eof, chunk.total_size, chunk.bytes_read);
            }
            Ok(())
        }
        "write" => {
            let size = client.write(args.path, args.text.as_bytes(), args.approved, false)?;
            println!("wrote {} ({size} bytes)", args.path);
            Ok(())
        }
        "delete" => {
            client.delete(args.path, args.approved)?;
            println!("deleted {}", args.path);
            Ok(())
        }
        other => {
            eprintln!("unknown file-cli command: {other} (list|read|write|delete)");
            std::process::exit(2);
        }
    }
}

fn run_local(args: &CliArgs) -> agent_desktop::Result<()> {
    let base = ScopedFileClient::new(std::path::PathBuf::from(args.local_root))?;
    let client = if args.approved { base.with_writes_allowed() } else { base };
    match args.command {
        "list" => {
            for entry in client.list(args.path)? {
                println!(
                    "  {} {} ({} bytes)",
                    if entry.is_dir { "dir " } else { "file" },
                    entry.path,
                    entry.size_bytes
                );
            }
            Ok(())
        }
        "read" => {
            if args.chunked {
                let data = client.read_all_chunked(args.path, args.len, 1_048_576)?;
                println!("{}", String::from_utf8_lossy(&data));
            } else {
                let chunk = client.read_chunk(ChunkedRead {
                    path: args.path.to_string(),
                    offset: args.offset,
                    len: args.len,
                    max_bytes: 1_048_576,
                    as_base64: false,
                })?;
                println!("{}", chunk.data);
                println!("eof={} bytes_read={}", chunk.eof, chunk.bytes_read);
            }
            Ok(())
        }
        "write" => {
            client.write(args.path, args.text.as_bytes())?;
            println!("wrote {}", args.path);
            Ok(())
        }
        "delete" => {
            client.delete(args.path)?;
            println!("deleted {}", args.path);
            Ok(())
        }
        other => {
            eprintln!("unknown file-cli command: {other} (list|read|write|delete)");
            std::process::exit(2);
        }
    }
}

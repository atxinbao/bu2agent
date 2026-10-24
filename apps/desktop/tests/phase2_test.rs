//! Phase 2 tests: session target switching, scoped local files,
//! and the file gateway client's 401 rules.
//!
//! Session: switching target invalidates the old target.
//! Local files: scope escape, approval gate, chunked read, max.
//! Gateway: reads retry once on 401 (refresh called once);
//! writes are never retried on 401 (sent exactly once).

use agent_desktop::file_client::{
    ChunkedRead, FileClient, ScopedFileClient,
};
use agent_desktop::file_gateway_client::FileGatewayClient;
use agent_desktop::gateway::GatewayTargetKey;
use agent_desktop::session::{SessionManager, SessionState};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn target(generation: u64) -> GatewayTargetKey {
    GatewayTargetKey {
        vm_name: "mock-vm-1".to_string(),
        gateway_url: "https://mock.local/gateway".to_string(),
        vm_id: None,
        generation,
    }
}

#[test]
fn session_switch_invalidates_old_target() {
    let mut session = SessionManager::new();
    session.activate(target(1));
    assert!(session.is_current_target(&target(1)));
    session.switch_target(target(2));
    assert_eq!(session.state, SessionState::SessionBootstrap);
    assert!(!session.is_current_target(&target(1)));
    assert!(session.is_current_target(&target(2)));
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("scoped-files-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn scoped_files_scope_approval_chunks_and_max() {
    let dir = temp_dir("phase2");
    let client = ScopedFileClient::new(dir.clone()).expect("client");
    // Unapproved writes are refused before touching disk.
    assert!(client.write("a.txt", b"hello world").is_err());
    assert!(!dir.join("a.txt").exists());
    let client = client.with_writes_allowed();
    client.write("a.txt", b"hello world").expect("write");
    client.write("sub/b.txt", b"second").expect("write sub");
    // Scope escape is rejected, both relative and absolute.
    assert!(client.write("../evil.txt", b"x").is_err());
    assert!(client.write("/tmp/evil.txt", b"x").is_err());
    assert!(client.read_chunk(ChunkedRead {
        path: "../evil.txt".to_string(),
        offset: 0,
        len: 10,
        max_bytes: 100,
        as_base64: false,
    }).is_err());
    // Chunked read: 5 bytes, not EOF; then rest, EOF.
    let first = client.read_chunk(ChunkedRead {
        path: "a.txt".to_string(),
        offset: 0,
        len: 5,
        max_bytes: 100,
        as_base64: false,
    }).expect("chunk 1");
    assert_eq!(first.data, "hello");
    assert!(!first.eof);
    assert_eq!(first.bytes_read, 5);
    let rest = client.read_chunk(ChunkedRead {
        path: "a.txt".to_string(),
        offset: 5,
        len: 100,
        max_bytes: 100,
        as_base64: true,
    }).expect("chunk 2");
    assert_eq!(rest.data, "IHdvcmxk");
    assert!(rest.eof);
    // read_all_chunked reassembles the file.
    let all = client.read_all_chunked("a.txt", 4, 100).expect("read all");
    assert_eq!(all, b"hello world");
    // max_bytes refuses an oversized file.
    assert!(client.read_chunk(ChunkedRead {
        path: "a.txt".to_string(),
        offset: 0,
        len: 100,
        max_bytes: 5,
        as_base64: false,
    }).is_err());
    // List is sorted and scoped.
    let listed = client.list("").expect("list");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].path, "a.txt");
    // Delete requires the approved client; this one is approved.
    client.delete("a.txt").expect("delete");
    assert!(!dir.join("a.txt").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

// Fake gateway: read returns 401 once then 200; write always 401.
fn start_fake_gateway() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake gateway");
    let addr = listener.local_addr().expect("fake gateway addr");
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let reads_clone = Arc::clone(&reads);
    let writes_clone = Arc::clone(&writes);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let text = String::from_utf8_lossy(&buf[..n]).into_owned();
            let path = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            let (status, body) = if path.starts_with("/files/read") {
                let count = reads_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    (401, r#"{"error":"unauthorized"}"#.to_string())
                } else {
                    (200, r#"{"path":"demo.txt","offset":0,"content_base64":"aGVsbG8=","eof":true,"total_size":5,"bytes_read":5}"#.to_string())
                }
            } else if path.starts_with("/files/write") {
                writes_clone.fetch_add(1, Ordering::SeqCst);
                (401, r#"{"error":"unauthorized"}"#.to_string())
            } else {
                (404, r#"{"error":"unknown"}"#.to_string())
            };
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}"), reads, writes)
}

#[test]
fn gateway_read_retries_once_on_401_write_never_retries() {
    let (base_url, reads, writes) = start_fake_gateway();
    let client = FileGatewayClient::new(&base_url).expect("client");
    let mut refresh_calls = 0usize;
    let chunk = client
        .read_chunk_with_refresh("demo.txt", 0, 100, &mut || {
            refresh_calls += 1;
            Ok(())
        })
        .expect("read after retry");
    assert_eq!(chunk.data, b"hello");
    assert!(chunk.eof);
    assert_eq!(refresh_calls, 1);
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    // Write is sent exactly once and fails on 401, no retry.
    let result = client.write("demo.txt", b"hello", true, false);
    assert!(result.is_err());
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    // Unapproved write never reaches the server at all.
    let result = client.write("demo.txt", b"hello", false, false);
    assert!(result.is_err());
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

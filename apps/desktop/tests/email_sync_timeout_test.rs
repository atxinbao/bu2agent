//! F-1 (D1-6a fix): `sync_mail` survives a slow gateway
//! response that exceeds the shared 10 s read timeout,
//! while the default-timeout calls still fail fast.
//!
//! The fake server delays every response 15 s: past the
//! 10 s default (the mailbox call fails) but well inside
//! the 120 s sync bound (the sync call succeeds). Only
//! success/failure outcomes are asserted, never tight
//! timing; margins are ~5 s on each side of the 10 s
//! boundary, and the whole test takes ~25 s.

use agent_desktop::email_client::EmailClient;
use std::io::{Read, Write};
use std::net::TcpListener;

const SLOW_RESPONSE_SECS: u64 = 15;

fn start_slow_fake_server(requests: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
    let addr = listener.local_addr().expect("fake server addr");
    std::thread::spawn(move || {
        let mut served = 0;
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
            // Delay past the 10 s default timeout, inside
            // the 120 s sync bound.
            std::thread::sleep(std::time::Duration::from_secs(SLOW_RESPONSE_SECS));
            let body = match path.as_str() {
                "/api/email/mailbox" => {
                    r#"{"configured":true,"address":"me@mock.invalid","name":null,"source":"mock"}"#
                }
                "/api/email/sync" => {
                    r#"{"synced_count":0,"skipped_count":51,"total_count":51,"cursor":"deadbeef"}"#
                }
                _ => r#"{"error":"unknown"}"#,
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            // The timed-out first client has already closed
            // its socket; ignore the write error.
            let _ = stream.write_all(response.as_bytes());
            served += 1;
            if served >= requests {
                break;
            }
        }
    });
    format!("http://{addr}")
}

#[test]
fn sync_mail_survives_slow_gateway_while_default_calls_time_out() {
    let base_url = start_slow_fake_server(2);
    let client = EmailClient::new(&base_url).expect("client");
    // Default 10 s client: the 15 s response never arrives.
    assert!(client.mailbox().is_err());
    // Sync-only 120 s client: the same 15 s delay succeeds.
    let result = client.sync_mail().expect("sync_mail");
    assert_eq!(result.synced_count, 0);
    assert_eq!(result.skipped_count, 51);
    assert_eq!(result.total_count, 51);
    assert_eq!(result.cursor, "deadbeef");
}

//! 任务 6 文件扩展端点冒烟：批量读取、搜索、raw、游标订阅、导出与媒体库。

use mock_gateway::server::{app, new_state_with_files_root};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start(root: std::path::PathBuf) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app(new_state_with_files_root(root))).await.unwrap(); });
    addr
}
async fn request(addr: std::net::SocketAddr, method: &str, path: &str, body: Option<&str>) -> (String, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let body = body.unwrap_or("");
    let content = if body.is_empty() { String::new() } else { format!("Content-Type: application/json\r\nContent-Length: {}\r\n", body.len()) };
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{content}\r\n{body}").as_bytes()).await.unwrap();
    let mut bytes = Vec::new(); s.read_to_end(&mut bytes).await.unwrap();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (head.lines().next().unwrap_or("").to_string(), body.to_string())
}
fn root(name: &str) -> std::path::PathBuf { let p = std::env::temp_dir().join(format!("task6-{name}-{}", std::process::id())); let _ = std::fs::remove_dir_all(&p); std::fs::create_dir_all(&p).unwrap(); p }
async fn post(addr: std::net::SocketAddr, path: &str, body: &str) -> (String, String) { request(addr, "POST", path, Some(body)).await }
async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) { request(addr, "GET", path, None).await }
async fn approve(addr: std::net::SocketAddr, tool: &str, params: &str) -> String {
    let (_, body) = post(addr, "/approval/request", &format!(r#"{{"tool":"{tool}",{params}}}"#)).await;
    let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["tool_call_id"].as_str().unwrap().to_string();
    let (status, _) = post(addr, "/approval/decide", &format!(r#"{{"tool_call_id":"{id}","status":"approved"}}"#)).await;
    assert!(status.contains("200")); id
}

#[tokio::test]
async fn extended_file_endpoints_keep_partial_results_and_jail() {
    let root = root("extended");
    std::fs::write(root.join("a.txt"), b"alpha").unwrap();
    std::fs::write(root.join("b.txt"), b"beta").unwrap();
    let addr = start(root.clone()).await;
    let (status, body) = post(addr, "/files/reads", r#"{"items":[{"path":"a.txt","len":5},{"path":"missing.txt","len":5}]}"#).await;
    assert!(status.contains("200")); let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["items"][0]["ok"], true); assert_eq!(value["items"][1]["ok"], false);
    let (status, body) = post(addr, "/files/stats", r#"{"paths":["a.txt","missing.txt"]}"#).await;
    assert!(status.contains("200")); let value: serde_json::Value = serde_json::from_str(&body).unwrap(); assert_eq!(value["items"][0]["stat"]["size_bytes"], 5);
    let (status, body) = post(addr, "/files/search", r#"{"query":"TXT","limit":1}"#).await;
    assert!(status.contains("200")); let value: serde_json::Value = serde_json::from_str(&body).unwrap(); assert_eq!(value["truncated"], true);
    let (status, body) = get(addr, "/files/raw?path=a.txt").await; assert!(status.contains("200")); assert!(body.ends_with("alpha"));
    let (status, body) = post(addr, "/files/subscribe", r#"{"path":"","wait_ms":0}"#).await; assert!(status.contains("200")); let cursor = serde_json::from_str::<serde_json::Value>(&body).unwrap()["cursor"].as_str().unwrap().to_string();
    let (status, _) = post(addr, "/files/subscribe", &format!(r#"{{"path":"","cursor":"{cursor}","wait_ms":1}}"#)).await; assert!(status.contains("200"));
    let export = std::env::temp_dir().join(format!("task6-export-{}.txt", std::process::id())); let _ = std::fs::remove_file(&export);
    let id = approve(addr, "fs.export", &format!(r#""path":"a.txt","to_path":"{}""#, export.display())).await;
    let (status, _) = post(addr, "/files/export", &format!(r#"{{"from":"a.txt","to":"{}","approval_id":"{id}"}}"#, export.display())).await; assert!(status.contains("200")); assert_eq!(std::fs::read(&export).unwrap(), b"alpha");
    let inside = root.join("inside.txt");
    let dotted_inside = root.join("..").join(root.file_name().unwrap()).join("inside.txt");
    let id = approve(addr, "fs.export", &format!(r#""path":"a.txt","to_path":"{}""#, dotted_inside.display())).await;
    let (status, _) = post(addr, "/files/export", &format!(r#"{{"from":"a.txt","to":"{}","approval_id":"{id}"}}"#, dotted_inside.display())).await;
    assert!(status.contains("400"), "dotted root-internal export must be rejected: {status}");
    assert!(!inside.exists());
    let (status, body) = post(addr, "/files/library", r#"{"limit":10}"#).await; assert!(status.contains("200")); assert!(body.contains("a.txt"));
    let png = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=").unwrap();
    std::fs::write(root.join("pixel.png"), png).unwrap();
    let (status, body) = get(addr, "/files/thumbnail?path=pixel.png&w=32&format=json").await; assert!(status.contains("200")); assert!(body.contains("image/jpeg"));
    std::fs::write(root.join("large.bin"), vec![0u8; 1_048_577]).unwrap();
    let (status, _) = get(addr, "/files/raw?path=large.bin").await; assert!(status.contains("413"));
    let _ = std::fs::remove_file(export); let _ = std::fs::remove_dir_all(root);
}

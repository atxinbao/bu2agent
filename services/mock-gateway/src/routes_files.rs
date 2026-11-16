//! - GET  /files/list            -> list a directory in the file root
//! - GET  /files/read            -> chunked read (offset/len, base64, EOF)
//! - POST /files/write           -> write/append a file in the root
//! - POST /files/delete          -> delete a file in the root
//! - GET  /files/stat            -> stat a file or directory
//! - POST /files/rename          -> move a file or directory
//! - POST /files/mkdir           -> create a directory
//! - POST /files/reads|stats     -> bounded batch read/stat
//! - POST /files/search          -> bounded name/path search
//! - GET  /files/raw             -> HTTP raw bytes (JSON variant for invoke)
//! - POST /files/subscribe       -> long-poll directory cursor
//! - GET  /files/thumbnail       -> bounded image thumbnail
//! - POST /files/export|library  -> bound external export and local media index
//!
//! File routes: /files (15 routes).
//!
//! The handlers moved here from `server.rs` (FU-5 S-7). The rest of the
//! route directory lives in `server.rs`; the 13 directory lines above
//! moved here with their handlers. `resolve_file` and `ensure_root` stay
//! in `server.rs` (already `pub(crate)`): staying code at ~5508 calls
//! them. The ledger/write-gate helpers (`record_effect`,
//! `transition_effect`, `bound_write_gate`, `consume_bound_approval`,
//! `consume_bound_approval_for_effect`, `start_bound_effect`,
//! `record_export_evidence`, `adapter_execution`) and `WriteGate` stay
//! in `server.rs` as `pub(crate)` and are imported from `crate::server`.
//! `FileEntryDto` is imported back by `server.rs` for the `AppState`
//! `files_last_tree` field type.

use std::io::Cursor;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::effect_store::NewEffect;
use crate::routes_common::bad_request;
use crate::server::{
    adapter_execution, bound_write_gate, consume_bound_approval,
    consume_bound_approval_for_effect, ensure_root, record_effect,
    record_export_evidence, resolve_file, start_bound_effect,
    transition_effect, AppState, WriteGate,
};
use crate::sqlite_store;

// --- File endpoints (Phase 2) ---

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FileEntryDto {
    name: String,
    path: String,
    is_dir: bool,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReadQuery {
    path: String,
    offset: Option<u64>,
    len: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadResponseDto {
    path: String,
    offset: u64,
    content_base64: String,
    eof: bool,
    total_size: u64,
    bytes_read: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WriteRequestDto {
    path: String,
    content_base64: String,
    append: Option<bool>,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteRequestDto {
    path: String,
    approval_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FileStatDto {
    path: String,
    is_dir: bool,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RenameRequestDto {
    from: String,
    to: String,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MkdirRequestDto {
    path: String,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchReadItemDto { path: String, offset: Option<u64>, len: Option<u64> }
#[derive(Debug, Deserialize)]
pub(crate) struct BatchReadRequestDto { items: Vec<BatchReadItemDto> }
#[derive(Debug, Serialize)]
pub(crate) struct BatchReadItemResponseDto { path: String, ok: bool, #[serde(skip_serializing_if = "Option::is_none")] result: Option<ReadResponseDto>, #[serde(skip_serializing_if = "Option::is_none")] error: Option<String> }
#[derive(Debug, Serialize)]
pub(crate) struct BatchReadResponseDto { items: Vec<BatchReadItemResponseDto> }
#[derive(Debug, Deserialize)]
pub(crate) struct BatchStatsRequestDto { paths: Vec<String> }
#[derive(Debug, Serialize)]
pub(crate) struct BatchStatItemResponseDto { path: String, ok: bool, #[serde(skip_serializing_if = "Option::is_none")] stat: Option<FileStatDto>, #[serde(skip_serializing_if = "Option::is_none")] error: Option<String> }
#[derive(Debug, Serialize)]
pub(crate) struct BatchStatsResponseDto { items: Vec<BatchStatItemResponseDto> }
#[derive(Debug, Deserialize)]
pub(crate) struct SearchRequestDto { query: String, start_path: Option<String>, limit: Option<usize>, max_depth: Option<usize> }
#[derive(Debug, Serialize)]
pub(crate) struct SearchResponseDto { entries: Vec<FileEntryDto>, truncated: bool }
#[derive(Debug, Deserialize)]
pub(crate) struct RawQuery { path: String, format: Option<String> }
#[derive(Debug, Deserialize)]
pub(crate) struct SubscribeRequestDto { path: Option<String>, cursor: Option<String>, wait_ms: Option<u64> }
#[derive(Debug, Serialize)]
pub(crate) struct SubscribeResponseDto { changed: bool, cursor: String, #[serde(skip_serializing_if = "Option::is_none")] entries: Option<Vec<FileEntryDto>> }
#[derive(Debug, Deserialize)]
pub(crate) struct ThumbnailQuery { path: String, w: Option<u32>, format: Option<String> }
#[derive(Debug, Deserialize)]
pub(crate) struct ExportRequestDto { from: String, to: String, approval_id: Option<String> }
#[derive(Debug, Deserialize)]
pub(crate) struct LibraryRequestDto { kinds: Option<Vec<String>>, limit: Option<usize> }
#[derive(Debug, Serialize)]
pub(crate) struct LibraryItemDto { path: String, kind: String, is_dir: bool, size_bytes: u64, modified_ms: u128 }
#[derive(Debug, Serialize)]
pub(crate) struct LibraryResponseDto { items: Vec<LibraryItemDto>, truncated: bool }

pub(crate) async fn files_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let rel = query.path.unwrap_or_default();
    let Ok(dir) = resolve_file(&state.files_root, &rel) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if ensure_root(&state.files_root).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response();
    }
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
        out.push(FileEntryDto {
            name,
            path,
            is_dir: metadata.is_dir(),
            size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Json(out).into_response()
}

pub(crate) async fn files_read(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> impl IntoResponse {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(data) = std::fs::read(&full) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response();
    };
    let offset = query.offset.unwrap_or(0);
    let len = query.len.unwrap_or(4096).min(1_048_576);
    let total = data.len() as u64;
    let start = (offset as usize).min(data.len());
    let end = start.saturating_add(len as usize).min(data.len());
    let chunk = &data[start..end];
    Json(ReadResponseDto {
        path: query.path,
        offset,
        content_base64: base64::engine::general_purpose::STANDARD.encode(chunk),
        eof: (end as u64) >= total,
        total_size: total,
        bytes_read: chunk.len() as u64,
    })
    .into_response()
}

pub(crate) async fn files_stat(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let rel = query.path.unwrap_or_default();
    let Ok(full) = resolve_file(&state.files_root, &rel) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(metadata) = std::fs::metadata(&full) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response();
    };
    Json(FileStatDto {
        path: rel,
        is_dir: metadata.is_dir(),
        size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
    }).into_response()
}

pub(crate) fn collect_tree(root: &Path, rel: &str, max_depth: usize) -> Result<Vec<FileEntryDto>, StatusCode> {
    let start = resolve_file(root, rel)?;
    let mut out = Vec::new();
    fn visit(dir: &Path, prefix: &str, depth: usize, max_depth: usize, out: &mut Vec<FileEntryDto>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            out.push(FileEntryDto { name, path: path.clone(), is_dir: meta.is_dir(), size_bytes: if meta.is_dir() { 0 } else { meta.len() } });
            if meta.is_dir() && depth < max_depth { visit(&entry.path(), &path, depth + 1, max_depth, out); }
        }
    }
    let _ = root;
    visit(&start, rel.trim_matches('/'), 0, max_depth, &mut out);
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub(crate) fn read_chunk_value(root: &Path, item: &BatchReadItemDto) -> Result<ReadResponseDto, String> {
    let full = resolve_file(root, &item.path).map_err(|_| "path escapes file root".to_string())?;
    let data = std::fs::read(&full).map_err(|_| "file not found".to_string())?;
    if full.is_dir() { return Err("cannot read directory".to_string()); }
    let offset = item.offset.unwrap_or(0);
    let len = item.len.unwrap_or(4096).min(1_048_576);
    let total = data.len() as u64;
    let start = (offset as usize).min(data.len());
    let end = start.saturating_add(len as usize).min(data.len());
    let chunk = &data[start..end];
    Ok(ReadResponseDto { path: item.path.clone(), offset, content_base64: base64::engine::general_purpose::STANDARD.encode(chunk), eof: end as u64 >= total, total_size: total, bytes_read: chunk.len() as u64 })
}

/// 对绝对目标做不依赖文件系统的词法折叠，避免 export
/// 用未经折叠的 `..` 绕过文件根前缀判断。
pub(crate) fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() { return None; }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                let Some(last) = normalized.file_name() else { continue };
                if last == std::ffi::OsStr::new("/") { return None; }
                normalized.pop();
            }
        }
    }
    Some(normalized)
}

pub(crate) async fn files_reads(State(state): State<AppState>, Json(req): Json<BatchReadRequestDto>) -> impl IntoResponse {
    const MAX_ITEMS: usize = 128;
    const MAX_TOTAL: u64 = 8 * 1_048_576;
    if req.items.len() > MAX_ITEMS { return bad_request("too many read items"); }
    let requested: u64 = req.items.iter().map(|i| i.len.unwrap_or(4096).min(1_048_576)).sum();
    if requested > MAX_TOTAL { return (StatusCode::PAYLOAD_TOO_LARGE, Json(serde_json::json!({"error":"batch read exceeds byte limit"}))).into_response(); }
    let items = req.items.iter().map(|item| match read_chunk_value(&state.files_root, item) {
        Ok(result) => BatchReadItemResponseDto { path: item.path.clone(), ok: true, result: Some(result), error: None },
        Err(error) => BatchReadItemResponseDto { path: item.path.clone(), ok: false, result: None, error: Some(error) },
    }).collect();
    Json(BatchReadResponseDto { items }).into_response()
}

pub(crate) async fn files_stats(State(state): State<AppState>, Json(req): Json<BatchStatsRequestDto>) -> impl IntoResponse {
    if req.paths.len() > 256 { return bad_request("too many stat paths"); }
    let items = req.paths.iter().map(|path| {
        let result = resolve_file(&state.files_root, path).ok().and_then(|full| std::fs::metadata(full).ok().map(|m| FileStatDto { path: path.clone(), is_dir: m.is_dir(), size_bytes: if m.is_dir() { 0 } else { m.len() } }));
        match result { Some(stat) => BatchStatItemResponseDto { path: path.clone(), ok: true, stat: Some(stat), error: None }, None => BatchStatItemResponseDto { path: path.clone(), ok: false, stat: None, error: Some("file not found or invalid path".to_string()) } }
    }).collect();
    Json(BatchStatsResponseDto { items }).into_response()
}

pub(crate) async fn files_search(State(state): State<AppState>, Json(req): Json<SearchRequestDto>) -> impl IntoResponse {
    let Ok(start) = resolve_file(&state.files_root, req.start_path.as_deref().unwrap_or("")) else { return bad_request("path escapes file root"); };
    let limit = req.limit.unwrap_or(200).clamp(1, 200);
    let depth = req.max_depth.unwrap_or(16).min(16);
    let query = req.query.to_lowercase();
    let mut all = collect_tree(&state.files_root, req.start_path.as_deref().unwrap_or(""), depth).unwrap_or_default();
    all.retain(|e| e.path.to_lowercase().contains(&query));
    let truncated = all.len() > limit;
    all.truncate(limit);
    let _ = start;
    Json(SearchResponseDto { entries: all, truncated }).into_response()
}

pub(crate) async fn files_raw(State(state): State<AppState>, Query(query): Query<RawQuery>) -> Response {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"path escapes file root"}))).into_response(); };
    let Ok(meta) = std::fs::metadata(&full) else { return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"file not found"}))).into_response(); };
    if meta.is_dir() { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"cannot read directory"}))).into_response(); }
    if meta.len() > 1_048_576 { return (StatusCode::PAYLOAD_TOO_LARGE, Json(serde_json::json!({"error":"file exceeds read limit"}))).into_response(); }
    let Ok(bytes) = std::fs::read(&full) else { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot read file"}))).into_response(); };
    let content_type = match full.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase().as_str() { "png" => "image/png", "jpg" | "jpeg" => "image/jpeg", "gif" => "image/gif", "webp" => "image/webp", "json" => "application/json", "txt" | "md" => "text/plain; charset=utf-8", _ => "application/octet-stream" };
    if query.format.as_deref() == Some("json") { return Json(serde_json::json!({"path":query.path,"content_type":content_type,"content_base64":base64::engine::general_purpose::STANDARD.encode(bytes)})).into_response(); }
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, content_type).header(header::CONTENT_DISPOSITION, "inline").body(Body::from(bytes)).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) fn tree_cursor(entries: &[FileEntryDto]) -> String { crate::operation_binding::sha256_hex(serde_json::to_string(entries).unwrap_or_default().as_bytes()) }

/// Paths added, removed, or size-changed between two
/// tree snapshots. Entries carry no mtime or content
/// hash, so a same-size rewrite is invisible to this
/// diff — the same limitation as the tree cursor itself
/// (a hash of these same entries).
pub(crate) fn diff_trees(old: &[FileEntryDto], new: &[FileEntryDto]) -> Vec<String> {
    use std::collections::HashMap;
    let old_map: HashMap<&str, u64> = old.iter().map(|e| (e.path.as_str(), e.size_bytes)).collect();
    let new_map: HashMap<&str, u64> = new.iter().map(|e| (e.path.as_str(), e.size_bytes)).collect();
    let mut out: Vec<String> = Vec::new();
    for (path, size) in &new_map {
        match old_map.get(path) {
            None => out.push((*path).to_string()),
            Some(old_size) if old_size != size => out.push((*path).to_string()),
            _ => {}
        }
    }
    for path in old_map.keys() {
        if !new_map.contains_key(path) {
            out.push((*path).to_string());
        }
    }
    out.sort();
    out.dedup();
    out
}

/// D1-4 (Q-D1-44): what `files_subscribe` observed, made
/// durable. The in-memory last tree is the diff baseline;
/// the durable record is one `file.changed` event per
/// changed path (path hash + old/new cursor, never file
/// content) plus the `files-watcher` cursor advance — all
/// in one SQLite transaction (see
/// `record_file_changes`). The first observation for a
/// path only establishes the baseline (no events), so a
/// restart re-establishes it instead of replaying. The
/// file source row's `cursor` tracks the root tree only;
/// subtree observations still emit events and advance the
/// `files-watcher` cursor. Best-effort when SQLite is
/// absent or broken: the subscribe response is unaffected.
pub(crate) fn note_tree_observation(state: &AppState, path: &str, entries: &[FileEntryDto], cursor: &str) {
    let old_entries = {
        let mut last = state.files_last_tree.lock().expect("files last tree lock");
        let old = last.get(path).cloned();
        last.insert(path.to_string(), entries.to_vec());
        old
    };
    let Some(old) = old_entries else {
        if path.is_empty() {
            if let Some(sqlite) = &state.sqlite {
                if let Ok(mut conn) = sqlite.lock() {
                    let _ = sqlite_store::update_source_cursor(&mut conn, "files-root", cursor);
                }
            }
        }
        return;
    };
    let old_cursor = tree_cursor(&old);
    if old_cursor == cursor {
        return;
    }
    let changed_paths = diff_trees(&old, entries);
    if changed_paths.is_empty() {
        return;
    }
    let Some(sqlite) = &state.sqlite else { return };
    let Ok(mut conn) = sqlite.lock() else { return };
    let hashes: Vec<String> = changed_paths
        .iter()
        .map(|p| sqlite_store::sha256_hex(p.as_bytes()))
        .collect();
    if let Err(detail) = sqlite_store::record_file_changes(
        &mut conn,
        "files-root",
        &hashes,
        &old_cursor,
        cursor,
        &state.origin_device_id,
    ) {
        eprintln!("warning: D1-4 file change recording failed: {detail}");
    }
}

pub(crate) async fn files_subscribe(State(state): State<AppState>, Json(req): Json<SubscribeRequestDto>) -> impl IntoResponse {
    let path = req.path.as_deref().unwrap_or("");
    let wait = req.wait_ms.unwrap_or(0).min(25_000);
    let mut entries = collect_tree(&state.files_root, path, 32).unwrap_or_default();
    let mut cursor = tree_cursor(&entries);
    if req.cursor.as_deref() != Some(cursor.as_str()) || wait == 0 { note_tree_observation(&state, path, &entries, &cursor); return Json(SubscribeResponseDto { changed: req.cursor.as_deref() != Some(cursor.as_str()), cursor, entries: Some(entries) }).into_response(); }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        entries = collect_tree(&state.files_root, path, 32).unwrap_or_default();
        cursor = tree_cursor(&entries);
        if req.cursor.as_deref() != Some(cursor.as_str()) || tokio::time::Instant::now() >= deadline { break; }
    }
    let changed = req.cursor.as_deref() != Some(cursor.as_str());
    note_tree_observation(&state, path, &entries, &cursor);
    Json(SubscribeResponseDto { changed, cursor, entries: if changed { Some(entries) } else { None } }).into_response()
}

pub(crate) async fn files_thumbnail(State(state): State<AppState>, Query(query): Query<ThumbnailQuery>) -> Response {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"path escapes file root"}))).into_response(); };
    let Ok(meta) = std::fs::metadata(&full) else { return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"file not found"}))).into_response(); };
    if meta.is_dir() { return (StatusCode::UNSUPPORTED_MEDIA_TYPE, Json(serde_json::json!({"error":"thumbnail supports images only"}))).into_response(); }
    let ext = full.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
    if !matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp") { return (StatusCode::UNSUPPORTED_MEDIA_TYPE, Json(serde_json::json!({"error":"thumbnail supports png, jpeg, gif and webp"}))).into_response(); }
    let Ok(bytes) = std::fs::read(&full) else { return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"error":"cannot read image"}))).into_response(); };
    let Ok(image) = image::load_from_memory(&bytes) else { return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"error":"cannot decode image"}))).into_response(); };
    let width = query.w.unwrap_or(256).clamp(1, 512);
    // Downscale only: a source that already fits the box keeps its size.
    let thumb = if image.width() > width || image.height() > width { image.thumbnail(width, width) } else { image };
    let mut output = Cursor::new(Vec::new());
    if thumb.write_to(&mut output, image::ImageFormat::Jpeg).is_err() { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot encode thumbnail"}))).into_response(); }
    let data = output.into_inner();
    if query.format.as_deref() == Some("json") { return Json(serde_json::json!({"path":query.path,"content_type":"image/jpeg","content_base64":base64::engine::general_purpose::STANDARD.encode(data)})).into_response(); }
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, "image/jpeg").header(header::CONTENT_DISPOSITION, "inline").body(Body::from(data)).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) async fn files_export(State(state): State<AppState>, Json(req): Json<ExportRequestDto>) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::export_digest(&req.from, &req.to) else { return bad_request("invalid export path"); };
    let Ok(source) = resolve_file(&state.files_root, &req.from) else { return bad_request("path escapes file root"); };
    let Some(target) = normalize_absolute_path(Path::new(&req.to)) else { return bad_request("export target must be an absolute path outside file root"); };
    let Ok(root) = std::fs::canonicalize(&state.files_root) else { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot resolve file root"}))).into_response(); };
    let target_abs = if target.exists() {
        std::fs::canonicalize(&target).unwrap_or(target.clone())
    } else if let (Some(parent), Some(name)) = (target.parent(), target.file_name()) {
        std::fs::canonicalize(parent).map(|p| p.join(name)).unwrap_or(target.clone())
    } else {
        target.clone()
    };
    if !target_abs.is_absolute() || target_abs.starts_with(&root) { return bad_request("export target must be outside file root"); }
    if target.exists() { return (StatusCode::CONFLICT, Json(serde_json::json!({"error":"export target already exists"}))).into_response(); }
    // P0-C: bound Effect gates the export (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            let size = std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0);
            return Json(serde_json::json!({"from":req.from,"to":req.to,"size_bytes":size,"effect_id":effect.effect_id,"replayed":true})).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.export", &req.approval_id, &digest, Some(&effect.effect_id)) { return *response; }
        if let Err(response) = start_bound_effect(&state, effect) { return *response; }
    } else if let Err(response) = consume_bound_approval(&state, "fs.export", &req.approval_id, &digest) { return *response; }
    if !source.is_file() { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"export source must be a file"}))).into_response(); }
    if let Some(parent) = target.parent() { if std::fs::create_dir_all(parent).is_err() { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot create export parent"}))).into_response(); } }
    match std::fs::copy(&source, &target) { Ok(bytes) => {
        // P0-C: drive the bound Effect; the scope is the
        // normalized source path (the digest already commits
        // to the absolute destination). Legacy approvals record
        // one terminal row.
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            // P0-R: the generic hook skips export rows (the
            // target path is not on the Effect row), so the
            // baseline is recorded here from the canonical
            // absolute target the copy actually wrote.
            record_export_evidence(&state, &effect.effect_id, effect.execution_id.as_deref(), &target_abs);
        } else {
            let scope = crate::operation_binding::normalize_path(&req.from).unwrap_or_default();
            let record = record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "export".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            record_export_evidence(&state, &record.effect_id, record.execution_id.as_deref(), &target_abs);
        }
        Json(serde_json::json!({"from":req.from,"to":req.to,"size_bytes":bytes})).into_response()
    }, Err(_) => { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot export file"}))).into_response() } }
}

pub(crate) async fn files_library(State(state): State<AppState>, Json(req): Json<LibraryRequestDto>) -> impl IntoResponse {
    let limit = req.limit.unwrap_or(200).clamp(1, 1000);
    let allowed = req.kinds.unwrap_or_default();
    let all = collect_tree(&state.files_root, "", 32).unwrap_or_default();
    let mut items = Vec::new();
    for entry in all.into_iter().filter(|e| !e.is_dir) {
        let ext = Path::new(&entry.path).extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
        let kind = if matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp") { "image" } else if matches!(ext.as_str(), "mp4" | "mov" | "mkv" | "avi") { "video" } else if matches!(ext.as_str(), "mp3" | "wav" | "m4a" | "flac") { "audio" } else { "document" }.to_string();
        if !allowed.is_empty() && !allowed.iter().any(|k| k == &kind) { continue; }
        let modified_ms = std::fs::metadata(state.files_root.join(&entry.path)).ok().and_then(|m| m.modified().ok()).and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis()).unwrap_or(0);
        items.push(LibraryItemDto { path: entry.path, kind, is_dir: false, size_bytes: entry.size_bytes, modified_ms });
    }
    items.sort_by(|a, b| b.modified_ms.cmp(&a.modified_ms).then_with(|| a.path.cmp(&b.path)));
    let truncated = items.len() > limit;
    items.truncate(limit);
    Json(LibraryResponseDto { items, truncated }).into_response()
}

pub(crate) async fn files_write(
    State(state): State<AppState>,
    Json(req): Json<WriteRequestDto>,
) -> impl IntoResponse {
    // The digest is computed from the normalized path and the decoded
    // bytes, then the bound approval is consumed before any write.
    // A bare call without an approval id is a 403, not a write.
    let Ok(data) = base64::engine::general_purpose::STANDARD.decode(&req.content_base64) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid base64 content" }))).into_response();
    };
    let Some(digest) = crate::operation_binding::write_digest(&req.path, &data, req.append.unwrap_or(false)) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    // P0-C: the bound Effect (planned at approval request)
    // gates the write: replay returns the prior Effect without
    // writing, an unsettled Effect refuses, a planned one is
    // consumed bound to it and driven forward.
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            let size = std::fs::metadata(
                state.files_root.join(
                    crate::operation_binding::normalize_path(&req.path).unwrap_or_default(),
                ),
            )
            .map(|m| m.len())
            .unwrap_or(0);
            return Json(serde_json::json!({ "path": req.path, "size_bytes": size, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.write", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.write", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            }
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
        }
    }
    let written: Result<u64, ()> = if req.append.unwrap_or(false) {
        (|| {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full)
                .map_err(|_| ())?;
            f.write_all(&data).map_err(|_| ())?;
            Ok(std::fs::metadata(&full).map(|m| m.len()).unwrap_or(data.len() as u64))
        })()
    } else {
        std::fs::write(&full, &data).map(|_| data.len() as u64).map_err(|_| ())
    };
    match written {
        Ok(size) => {
            // P0-C: drive the bound Effect to its terminal
            // state; legacy approvals record one terminal row.
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "write".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "path": req.path, "size_bytes": size })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "write".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response()
        }
    }
}

pub(crate) async fn files_delete(
    State(state): State<AppState>,
    Json(req): Json<DeleteRequestDto>,
) -> impl IntoResponse {
    // Same bound gate as files_write: no approval id, no delete.
    let Some(digest) = crate::operation_binding::delete_digest(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    // P0-C: bound Effect gates the delete (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "deleted": true, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.delete", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.delete", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if full.is_dir() {
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
        }
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "delete refuses directories" }))).into_response();
    }
    match std::fs::remove_file(&full) {
        Ok(()) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "deleted": true })).into_response()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response()
        }
    }
}

pub(crate) async fn files_rename(
    State(state): State<AppState>,
    Json(req): Json<RenameRequestDto>,
) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::rename_digest(&req.from, &req.to) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(from) = resolve_file(&state.files_root, &req.from) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(to) = resolve_file(&state.files_root, &req.to) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if req.approval_id.is_none() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "bound approval required" }))).into_response();
    }
    if to.exists() {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "rename target already exists" }))).into_response();
    }
    // P0-C: bound Effect gates the rename (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "from": req.from, "to": req.to, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.rename", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.rename", &req.approval_id, &digest) {
        return *response;
    }
    if !from.exists() {
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
        }
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "source file not found" }))).into_response();
    }
    if let Some(parent) = to.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            }
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
        }
    }
    match std::fs::rename(&from, &to) {
        Ok(()) => {
            // P0-C: drive the bound Effect; the scope names both
            // ends, normalized, in order. Legacy approvals record
            // one terminal row.
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                let scope = format!(
                    "{} -> {}",
                    crate::operation_binding::normalize_path(&req.from).unwrap_or_default(),
                    crate::operation_binding::normalize_path(&req.to).unwrap_or_default()
                );
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "rename".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "from": req.from, "to": req.to })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                let scope = format!(
                    "{} -> {}",
                    crate::operation_binding::normalize_path(&req.from).unwrap_or_default(),
                    crate::operation_binding::normalize_path(&req.to).unwrap_or_default()
                );
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "rename".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot rename file" }))).into_response()
        }
    }
}

pub(crate) async fn files_mkdir(
    State(state): State<AppState>,
    Json(req): Json<MkdirRequestDto>,
) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::mkdir_digest(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    // P0-C: bound Effect gates the mkdir (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "path": req.path, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.mkdir", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.mkdir", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    match std::fs::create_dir_all(&full) {
        Ok(()) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "mkdir".to_string(), scope: normalized.clone(), input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "path": normalized })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "mkdir".to_string(), scope: normalized.clone(), input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create directory" }))).into_response()
        }
    }
}

//! 循环审计只存结构字段；模型上下文、工具正文与错误正文不进入此类型。

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use crate::loop_engine::LoopRecord;

pub fn load(path: &Path) -> Result<Vec<LoopRecord>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err("loop_audit_read_failed".into()),
    };
    let mut records = BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        // 不跳过未知的最后状态，以免把旧快照当成可执行状态。
        let record: LoopRecord =
            serde_json::from_str(line).map_err(|_| "loop_audit_invalid".to_string())?;
        records.insert(record.loop_id.clone(), record);
    }
    Ok(records.into_values().collect())
}

pub fn append(path: &Path, record: &LoopRecord) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|_| "loop_audit_write_failed")?;
    }
    let mut line = serde_json::to_vec(record).map_err(|_| "loop_audit_encode_failed")?;
    line.push(b'\n');
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(&line))
        .map_err(|_| "loop_audit_write_failed".to_string())
}

/// 文件根与日志路径先解析现有祖先（包括符号链接），再比较包含关系。
pub fn outside_root(root: &Path, path: &Path) -> Result<(), String> {
    fn absolute(path: &Path) -> Result<std::path::PathBuf, String> {
        let absolute = std::path::absolute(path).map_err(|_| "invalid_loop_path")?;
        let mut base = absolute.as_path();
        let mut tail = Vec::new();
        while !base.exists() {
            tail.push(base.file_name().ok_or("invalid_loop_path")?.to_owned());
            base = base.parent().ok_or("invalid_loop_path")?;
        }
        let mut out = base.canonicalize().map_err(|_| "invalid_loop_path")?;
        for part in tail.into_iter().rev() {
            out.push(part);
        }
        Ok(out)
    }
    if path
        .components()
        .any(|p| matches!(p, std::path::Component::ParentDir))
    {
        return Err("invalid_loop_path".into());
    }
    if absolute(path)?.starts_with(absolute(root)?) {
        return Err("loop_audit_inside_file_root".into());
    }
    Ok(())
}

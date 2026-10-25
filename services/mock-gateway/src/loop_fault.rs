//! 仅供专用临时目录验收的循环文件分派装饰器，不合成工具结果或修改状态机。

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::response::Response;

use crate::loop_engine::Timeouts;

#[derive(Clone, Debug)]
pub enum FaultMode {
    WriteDelay(Duration),
    RemoveBeforeReadback,
}

#[derive(Clone, Default)]
pub struct LoopFault {
    mode: Option<FaultMode>,
    root: Option<PathBuf>,
    tool_timeout: Option<Duration>,
}

impl LoopFault {
    /// 仅生产启动读取环境；单测使用显式配置，避免全局环境污染并行测试。
    pub fn from_env(root: &Path) -> Result<Self, String> {
        let mode = match std::env::var("MOCK_LOOP_FAULT") {
            Err(std::env::VarError::NotPresent) => return Ok(Self::default()),
            Err(_) => return Err("invalid MOCK_LOOP_FAULT".into()),
            Ok(value) => value,
        };
        let mode = match mode.as_str() {
            "write_delay" => FaultMode::WriteDelay(Self::env_ms("MOCK_LOOP_WRITE_DELAY_MS")?),
            "readback_missing" => FaultMode::RemoveBeforeReadback,
            _ => return Err("invalid MOCK_LOOP_FAULT".into()),
        };
        let timeout = if std::env::var_os("MOCK_LOOP_TOOL_TIMEOUT_MS").is_some() {
            Some(Self::env_ms("MOCK_LOOP_TOOL_TIMEOUT_MS")?)
        } else {
            None
        };
        Self::new(root, mode, timeout)
    }

    fn env_ms(name: &str) -> Result<Duration, String> {
        let ms = std::env::var(name)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|ms| *ms > 0 && *ms <= 60_000)
            .ok_or_else(|| format!("invalid {name}"))?;
        Ok(Duration::from_millis(ms))
    }

    pub fn new(
        root: &Path,
        mode: FaultMode,
        tool_timeout: Option<Duration>,
    ) -> Result<Self, String> {
        let root = root.canonicalize().map_err(|_| "fault root must exist")?;
        let temporary = std::env::temp_dir()
            .canonicalize()
            .map_err(|_| "missing temporary root")?;
        if root == temporary || !root.starts_with(&temporary) {
            return Err("loop fault requires a dedicated temporary file root".into());
        }
        if let FaultMode::WriteDelay(delay) = &mode {
            if delay.is_zero() || *delay > Duration::from_secs(60) {
                return Err("invalid loop write delay".into());
            }
        }
        if tool_timeout.is_some_and(|t| t.is_zero() || t > Timeouts::default().tool) {
            return Err("loop fault timeout can only decrease the tool limit".into());
        }
        Ok(Self {
            mode: Some(mode),
            root: Some(root),
            tool_timeout,
        })
    }

    pub fn timeouts(&self) -> Timeouts {
        let mut timeouts = Timeouts::default();
        if let Some(tool) = self.tool_timeout {
            timeouts.tool = tool;
        }
        timeouts
    }

    /// 仅循环执行侧在审批消费后调用；读与无配置分支原样委托。
    pub fn dispatch(
        &self,
        write_path: Option<&Path>,
        dispatch: impl FnOnce() -> Response,
    ) -> Response {
        if write_path.is_some() {
            if let Some(FaultMode::WriteDelay(delay)) = self.mode {
                std::thread::sleep(delay);
            }
        }
        let response = dispatch();
        if response.status().is_success()
            && matches!(self.mode, Some(FaultMode::RemoveBeforeReadback))
        {
            if let Some(path) = write_path {
                // 再核对实际目标仍在约定临时根内，只破坏本次刚写的文件。
                if let Ok(target) = path.canonicalize() {
                    if self
                        .root
                        .as_ref()
                        .is_some_and(|root| target.starts_with(root) && target != *root)
                        && std::fs::remove_file(target).is_err()
                    {
                        eprintln!("warning: loop readback fault could not remove test target");
                    }
                }
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[test]
    fn fault_requires_dedicated_temp_root_and_lower_timeout() {
        let temp = std::env::temp_dir();
        let root = temp.join(format!("bu2agent-fault-config-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        assert!(LoopFault::new(&temp, FaultMode::RemoveBeforeReadback, None).is_err());
        assert!(LoopFault::new(
            temp.parent().unwrap(),
            FaultMode::RemoveBeforeReadback,
            None
        )
        .is_err());
        for timeout in [Duration::ZERO, Duration::from_secs(11)] {
            assert!(LoopFault::new(&root, FaultMode::RemoveBeforeReadback, Some(timeout)).is_err());
        }
        assert!(LoopFault::new(&root, FaultMode::WriteDelay(Duration::ZERO), None).is_err());
        let config = LoopFault::new(
            &root,
            FaultMode::RemoveBeforeReadback,
            Some(Duration::from_millis(200)),
        )
        .unwrap();
        assert_eq!(config.timeouts().tool, Duration::from_millis(200));
        assert_eq!(config.timeouts().model, Duration::from_secs(30));
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn default_delegates_once_without_deleting_and_keeps_normal_timeouts() {
        let file =
            std::env::temp_dir().join(format!("bu2agent-fault-default-{}", std::process::id()));
        std::fs::write(&file, "default retained").unwrap();
        let config = LoopFault::default();
        let mut calls = 0;
        let response = config.dispatch(Some(&file), || {
            calls += 1;
            StatusCode::OK.into_response()
        });
        assert_eq!(calls, 1);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "default retained");
        assert_eq!(config.timeouts().tool, Duration::from_secs(10));
        std::fs::remove_file(file).unwrap();
    }

    #[test]
    fn removal_only_follows_successful_write_dispatch() {
        let root =
            std::env::temp_dir().join(format!("bu2agent-fault-remove-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("out");
        std::fs::write(&file, "previous").unwrap();
        let config = LoopFault::new(&root, FaultMode::RemoveBeforeReadback, None).unwrap();
        config.dispatch(Some(&file), || StatusCode::BAD_REQUEST.into_response());
        assert!(file.exists());
        config.dispatch(None, || StatusCode::OK.into_response());
        assert!(file.exists());
        let response = config.dispatch(Some(&file), || {
            std::fs::write(&file, "new").unwrap();
            StatusCode::OK.into_response()
        });
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!file.exists());
        std::fs::remove_dir(root).unwrap();
    }
}

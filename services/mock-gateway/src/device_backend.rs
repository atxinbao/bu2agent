//! Reminder device backend trait, Fake implementation, and the
//! execute/refresh lifecycle shared by the real backend.
//!
//! Design rules (see device-loop-design.md):
//! - `set_completed` sets an explicit state; it is idempotent by contract.
//!   A double set to the same value must leave the state unchanged.
//! - Authorization is a precondition, never a per-write approval: the
//!   bu2agent approval record gates every write. When the backend reports
//!   NotDetermined, execution yields AwaitingAuthorization (the UI shows
//!   "waiting for authorization") and no write is attempted.
//! - A write submitted but unconfirmed within the timeout yields Unknown,
//!   and so does a backend-reported Timeout: a timeout never proves
//!   failure, it only proves the outcome was not observed.
//!   Only a successful re-query showing the requested state resolves
//!   Unknown. A failed re-query leaves it Unknown (with the query error
//!   recorded as detail), and so does a successful re-query showing the
//!   old state: after a timeout the write may still be in flight, so an
//!   unchanged state is not evidence of failure. A missing reminder is
//!   the one re-query outcome that fails Unknown: a write cannot land
//!   on a reminder that no longer exists.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reminder {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationStatus {
    Authorized,
    Denied,
    NotDetermined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceErrorKind {
    NotFound,
    AuthorizationDenied,
    Unavailable,
    Timeout,
    Other,
}

impl DeviceErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceErrorKind::NotFound => "not_found",
            DeviceErrorKind::AuthorizationDenied => "authorization_denied",
            DeviceErrorKind::Unavailable => "unavailable",
            DeviceErrorKind::Timeout => "timeout",
            DeviceErrorKind::Other => "other",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceError {
    pub kind: DeviceErrorKind,
    pub message: String,
}

impl DeviceError {
    pub fn new(kind: DeviceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for DeviceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for DeviceError {}

pub trait ReminderBackend: Send + Sync {
    fn authorization_status(&self) -> AuthorizationStatus;
    /// Ask the system for reminders access (shows the OS prompt
    /// when the status is NotDetermined) and return the resulting
    /// status. The default is a no-op that re-reads the status:
    /// the Fake backend has no system permission to request.
    /// The EventKit backend overrides this with a real request.
    fn request_authorization(&self) -> AuthorizationStatus {
        self.authorization_status()
    }
    fn list(&self) -> Result<Vec<Reminder>, DeviceError>;
    fn get(&self, id: &str) -> Result<Option<Reminder>, DeviceError>;
    fn set_completed(&self, id: &str, completed: bool) -> Result<Reminder, DeviceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteOutcome {
    Completed(Reminder),
    Failed(DeviceError),
    Unknown,
    AwaitingAuthorization,
}

/// Execute one set_completed call behind the approval gate's caller.
/// Authorization is checked first: NotDetermined yields
/// AwaitingAuthorization with no write attempted, Denied yields Failed.
/// The write itself runs on a detached thread with a timeout: if the
/// timeout fires first, the outcome is Unknown even though the write
/// may still land later -- that is exactly why Unknown must be resolved
/// by re-querying instead of by retrying blindly.
pub fn execute_set(
    backend: &Arc<dyn ReminderBackend>,
    id: &str,
    completed: bool,
    timeout: Duration,
) -> ExecuteOutcome {
    match backend.authorization_status() {
        AuthorizationStatus::NotDetermined => return ExecuteOutcome::AwaitingAuthorization,
        AuthorizationStatus::Denied => {
            return ExecuteOutcome::Failed(DeviceError::new(
                DeviceErrorKind::AuthorizationDenied,
                "reminders access denied",
            ));
        }
        AuthorizationStatus::Authorized => {}
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let backend = Arc::clone(backend);
    let id_owned = id.to_string();
    std::thread::spawn(move || {
        let outcome = match backend.set_completed(&id_owned, completed) {
            Ok(_) => {
                // 保存响应可能只是本地对象，必须重新读取；写入与重查共享超时预算。
                match backend.get(&id_owned) {
                    Ok(Some(reminder))
                        if reminder.id == id_owned && reminder.completed == completed =>
                    {
                        ExecuteOutcome::Completed(reminder)
                    }
                    // 保存已经提交，旧状态、缺失或查询失败均不能证明未写入。
                    _ => ExecuteOutcome::Unknown,
                }
            }
            Err(e) if e.kind == DeviceErrorKind::Timeout => ExecuteOutcome::Unknown,
            Err(e) => ExecuteOutcome::Failed(e),
        };
        let _ = tx.send(outcome);
    });
    match rx.recv_timeout(timeout) {
        Ok(outcome) => outcome,
        Err(_) => ExecuteOutcome::Unknown,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveOutcome {
    Completed(Reminder),
    Failed(DeviceError),
    StillUnknown(Option<DeviceError>),
}

/// Resolve an Unknown action by re-querying. Only a successful read may
/// change the outcome: a state match completes it, and a missing reminder
/// fails it, because a write cannot land on a reminder that no longer
/// exists -- so a missing record excludes a late write. A state mismatch
/// or a query error leaves it Unknown: after a timeout the write may
/// still be in flight, so an unchanged state is not evidence of failure.
pub fn resolve_unknown(
    backend: &Arc<dyn ReminderBackend>,
    id: &str,
    target_completed: bool,
) -> ResolveOutcome {
    match backend.get(id) {
        Ok(Some(reminder)) => {
            if reminder.completed == target_completed {
                ResolveOutcome::Completed(reminder)
            } else {
                // The write may still be in flight (a timeout does not
                // mean the write failed), so an unchanged state keeps
                // Unknown until a later re-query proves otherwise.
                ResolveOutcome::StillUnknown(None)
            }
        }
        Ok(None) => ResolveOutcome::Failed(DeviceError::new(
            DeviceErrorKind::NotFound,
            "reminder not found",
        )),
        Err(e) => ResolveOutcome::StillUnknown(Some(e)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeFault {
    Unavailable,
    Timeout,
}

pub struct FakeReminderBackend {
    reminders: Mutex<HashMap<String, Reminder>>,
    auth: Mutex<AuthorizationStatus>,
    fault: Mutex<Option<FakeFault>>,
    delay: Mutex<Duration>,
}

impl FakeReminderBackend {
    pub fn new() -> Self {
        Self {
            reminders: Mutex::new(HashMap::new()),
            auth: Mutex::new(AuthorizationStatus::Authorized),
            fault: Mutex::new(None),
            delay: Mutex::new(Duration::ZERO),
        }
    }

    pub fn add_reminder(&self, reminder: Reminder) {
        self.reminders
            .lock()
            .unwrap()
            .insert(reminder.id.clone(), reminder);
    }

    pub fn set_auth(&self, status: AuthorizationStatus) {
        *self.auth.lock().unwrap() = status;
    }

    pub fn set_fault(&self, fault: Option<FakeFault>) {
        *self.fault.lock().unwrap() = fault;
    }

    pub fn set_delay(&self, delay: Duration) {
        *self.delay.lock().unwrap() = delay;
    }

    fn fail_or_delay(&self) -> Result<(), DeviceError> {
        let delay = *self.delay.lock().unwrap();
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        match *self.fault.lock().unwrap() {
            Some(FakeFault::Unavailable) => {
                return Err(DeviceError::new(
                    DeviceErrorKind::Unavailable,
                    "fake backend unavailable",
                ));
            }
            Some(FakeFault::Timeout) => {
                return Err(DeviceError::new(
                    DeviceErrorKind::Timeout,
                    "fake backend timed out",
                ));
            }
            None => {}
        }
        Ok(())
    }
}

impl Default for FakeReminderBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl ReminderBackend for FakeReminderBackend {
    fn authorization_status(&self) -> AuthorizationStatus {
        *self.auth.lock().unwrap()
    }

    fn list(&self) -> Result<Vec<Reminder>, DeviceError> {
        self.fail_or_delay()?;
        let mut items: Vec<Reminder> = self.reminders.lock().unwrap().values().cloned().collect();
        items.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(items)
    }

    fn get(&self, id: &str) -> Result<Option<Reminder>, DeviceError> {
        self.fail_or_delay()?;
        Ok(self.reminders.lock().unwrap().get(id).cloned())
    }

    fn set_completed(&self, id: &str, completed: bool) -> Result<Reminder, DeviceError> {
        self.fail_or_delay()?;
        let mut map = self.reminders.lock().unwrap();
        match map.get_mut(id) {
            Some(reminder) => {
                reminder.completed = completed;
                Ok(reminder.clone())
            }
            None => Err(DeviceError::new(
                DeviceErrorKind::NotFound,
                "reminder not found",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_with_reminder() -> Arc<dyn ReminderBackend> {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        Arc::new(fake)
    }

    // 模拟保存返回目标对象，但真正的设备查询不一定与它一致。
    struct ReadbackBackend {
        result: Result<Option<Reminder>, DeviceError>,
        delay: Duration,
    }

    impl ReminderBackend for ReadbackBackend {
        fn authorization_status(&self) -> AuthorizationStatus {
            AuthorizationStatus::Authorized
        }
        fn list(&self) -> Result<Vec<Reminder>, DeviceError> {
            unreachable!()
        }
        fn get(&self, _: &str) -> Result<Option<Reminder>, DeviceError> {
            std::thread::sleep(self.delay);
            self.result.clone()
        }
        fn set_completed(&self, id: &str, completed: bool) -> Result<Reminder, DeviceError> {
            Ok(Reminder {
                id: id.into(),
                title: "local payload".into(),
                completed,
            })
        }
    }

    #[test]
    fn save_success_with_old_state_is_unknown() {
        let backend: Arc<dyn ReminderBackend> = Arc::new(ReadbackBackend {
            result: Ok(Some(Reminder {
                id: "reminder-1".into(),
                title: "actual".into(),
                completed: true,
            })),
            delay: Duration::ZERO,
        });
        assert_eq!(
            execute_set(&backend, "reminder-1", false, Duration::from_secs(1)),
            ExecuteOutcome::Unknown
        );
    }

    #[test]
    fn save_success_with_missing_or_failed_readback_is_unknown() {
        for result in [
            Ok(None),
            Err(DeviceError::new(
                DeviceErrorKind::Unavailable,
                "read failed",
            )),
        ] {
            let backend: Arc<dyn ReminderBackend> = Arc::new(ReadbackBackend {
                result,
                delay: Duration::ZERO,
            });
            assert_eq!(
                execute_set(&backend, "reminder-1", false, Duration::from_secs(1)),
                ExecuteOutcome::Unknown
            );
        }
    }

    #[test]
    fn completion_returns_actual_readback_not_local_payload() {
        let actual = Reminder {
            id: "reminder-1".into(),
            title: "actual".into(),
            completed: false,
        };
        let backend: Arc<dyn ReminderBackend> = Arc::new(ReadbackBackend {
            result: Ok(Some(actual.clone())),
            delay: Duration::ZERO,
        });
        assert_eq!(
            execute_set(&backend, "reminder-1", false, Duration::from_secs(1)),
            ExecuteOutcome::Completed(actual)
        );
    }

    #[test]
    fn slow_readback_uses_same_timeout_budget() {
        let backend: Arc<dyn ReminderBackend> = Arc::new(ReadbackBackend {
            result: Ok(None),
            delay: Duration::from_millis(100),
        });
        assert_eq!(
            execute_set(&backend, "reminder-1", false, Duration::from_millis(10)),
            ExecuteOutcome::Unknown
        );
    }

    #[test]
    fn fake_set_completed_roundtrip() {
        let backend = fake_with_reminder();
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_secs(5));
        match outcome {
            ExecuteOutcome::Completed(reminder) => assert!(reminder.completed),
            other => panic!("expected Completed, got {other:?}"),
        }
        assert!(backend.get("reminder-1").unwrap().unwrap().completed);
    }

    #[test]
    fn fake_double_complete_is_idempotent() {
        let backend = fake_with_reminder();
        for _ in 0..2 {
            let outcome = execute_set(&backend, "reminder-1", true, Duration::from_secs(5));
            assert!(matches!(outcome, ExecuteOutcome::Completed(_)));
        }
        assert!(backend.get("reminder-1").unwrap().unwrap().completed);
        for _ in 0..2 {
            let outcome = execute_set(&backend, "reminder-1", false, Duration::from_secs(5));
            assert!(matches!(outcome, ExecuteOutcome::Completed(_)));
        }
        assert!(!backend.get("reminder-1").unwrap().unwrap().completed);
    }

    #[test]
    fn execute_awaiting_authorization_attempts_no_write() {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        fake.set_auth(AuthorizationStatus::NotDetermined);
        let backend: Arc<dyn ReminderBackend> = Arc::new(fake);
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_secs(5));
        assert_eq!(outcome, ExecuteOutcome::AwaitingAuthorization);
        assert!(!backend.get("reminder-1").unwrap().unwrap().completed);
    }

    #[test]
    fn execute_denied_fails_without_write() {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        fake.set_auth(AuthorizationStatus::Denied);
        let backend: Arc<dyn ReminderBackend> = Arc::new(fake);
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_secs(5));
        match outcome {
            ExecuteOutcome::Failed(e) => {
                assert_eq!(e.kind, DeviceErrorKind::AuthorizationDenied);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        assert!(!backend.get("reminder-1").unwrap().unwrap().completed);
    }

    #[test]
    fn execute_timeout_yields_unknown() {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        fake.set_delay(Duration::from_millis(200));
        let backend: Arc<dyn ReminderBackend> = Arc::new(fake);
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_millis(10));
        assert_eq!(outcome, ExecuteOutcome::Unknown);
    }

    #[test]
    fn execute_not_found_fails() {
        let backend = fake_with_reminder();
        let outcome = execute_set(&backend, "missing", true, Duration::from_secs(5));
        match outcome {
            ExecuteOutcome::Failed(e) => assert_eq!(e.kind, DeviceErrorKind::NotFound),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn resolve_unknown_query_failure_keeps_unknown() {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: true,
        });
        fake.set_fault(Some(FakeFault::Unavailable));
        let backend: Arc<dyn ReminderBackend> = Arc::new(fake);
        match resolve_unknown(&backend, "reminder-1", true) {
            ResolveOutcome::StillUnknown(Some(e)) => {
                assert_eq!(e.kind, DeviceErrorKind::Unavailable);
            }
            other => panic!("expected StillUnknown, got {other:?}"),
        }
    }

    #[test]
    fn resolve_unknown_state_match_completes() {
        let backend = fake_with_reminder();
        let _ = backend.set_completed("reminder-1", true);
        match resolve_unknown(&backend, "reminder-1", true) {
            ResolveOutcome::Completed(reminder) => assert!(reminder.completed),
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    fn resolve_unknown_state_mismatch_keeps_unknown() {
        // The write may still be in flight after a timeout, so an
        // unchanged state is not evidence of failure.
        let backend = fake_with_reminder();
        match resolve_unknown(&backend, "reminder-1", true) {
            ResolveOutcome::StillUnknown(None) => {}
            other => panic!("expected StillUnknown, got {other:?}"),
        }
    }

    #[test]
    fn late_write_resolves_unknown_to_completed() {
        // Regression: a write that lands after the timeout must not have
        // been judged Failed by an earlier re-query that saw the old state.
        let concrete = Arc::new(FakeReminderBackend::new());
        concrete.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        concrete.set_delay(Duration::from_millis(100));
        let backend: Arc<dyn ReminderBackend> = concrete.clone();
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_millis(10));
        assert_eq!(outcome, ExecuteOutcome::Unknown);
        // Reads recover immediately while the write is still in flight:
        // clearing the delay does not disturb the write thread's sleep.
        concrete.set_delay(Duration::ZERO);
        match resolve_unknown(&backend, "reminder-1", true) {
            ResolveOutcome::StillUnknown(_) => {}
            other => panic!("expected StillUnknown, got {other:?}"),
        }
        assert!(!backend.get("reminder-1").unwrap().unwrap().completed);
        // The late write lands; the next re-query resolves to Completed.
        std::thread::sleep(Duration::from_millis(300));
        match resolve_unknown(&backend, "reminder-1", true) {
            ResolveOutcome::Completed(reminder) => assert!(reminder.completed),
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    fn backend_timeout_error_yields_unknown() {
        let fake = FakeReminderBackend::new();
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "bu2agent test".to_string(),
            completed: false,
        });
        fake.set_fault(Some(FakeFault::Timeout));
        let backend: Arc<dyn ReminderBackend> = Arc::new(fake);
        let outcome = execute_set(&backend, "reminder-1", true, Duration::from_secs(5));
        assert_eq!(outcome, ExecuteOutcome::Unknown);
    }

    #[test]
    fn resolve_unknown_missing_fails() {
        let backend = fake_with_reminder();
        match resolve_unknown(&backend, "missing", true) {
            ResolveOutcome::Failed(e) => assert_eq!(e.kind, DeviceErrorKind::NotFound),
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}

//! EventKit backend for the Mac.
//!
//! Uses the `eventkit` crate's synchronous API: authorization status
//! reads, full-access requests, reminder fetches, and reminder saves.
//! The store is constructed per call, so this backend holds no
//! EventKit state and is trivially Send + Sync.
//!
//! Writes go through the gateway's approval loop: the server
//! re-checks an approved approval record before `set_completed`
//! is ever called, and the allowlist pin limits which reminder
//! it may touch. This backend itself performs no approval --
//! it is the executing tool, not the gate.
//!
//! Privacy: this backend never logs reminder titles or notes.
//! Verification output must print counts only, never content.
//! (The crate itself redacts titles in its Debug output.)

use eventkit::error::{EKAuthorizationStatus, EventKitError};
use eventkit::event_store::{EKEntityType, EKEventStore, EKReminderPredicate};
use eventkit::reminder::EKReminder;

use crate::device_backend::{
    AuthorizationStatus, DeviceError, DeviceErrorKind, Reminder, ReminderBackend,
};

/// EventKit-backed reminders. Unit struct: the store is built per call.
pub struct EventKitReminderBackend;

fn map_status(status: EKAuthorizationStatus) -> AuthorizationStatus {
    match status {
        EKAuthorizationStatus::FullAccess => AuthorizationStatus::Authorized,
        EKAuthorizationStatus::NotDetermined => AuthorizationStatus::NotDetermined,
        // Restricted, Denied, WriteOnly, and any unknown raw status
        // cannot be trusted as readable access: fail closed.
        _ => AuthorizationStatus::Denied,
    }
}

fn map_error(error: EventKitError) -> DeviceError {
    DeviceError::new(DeviceErrorKind::Unavailable, error.to_string())
}

fn open_store() -> Result<EKEventStore, DeviceError> {
    EKEventStore::new().map_err(map_error)
}

fn prepare_completed(item: &mut EKReminder, completed: bool) {
    // Swift 桥优先使用 completion_date；清空它才能让显式状态生效。
    item.completion_date = None;
    item.is_completed = completed;
}

impl ReminderBackend for EventKitReminderBackend {
    fn authorization_status(&self) -> AuthorizationStatus {
        map_status(EKEventStore::authorization_status(EKEntityType::Reminder))
    }

    fn request_authorization(&self) -> AuthorizationStatus {
        // Synchronous request: waits up to 30 seconds for the
        // answer, then returns TimedOut while the prompt may
        // still be showing. Either way, re-read the status:
        // that is the authoritative answer.
        let store = match open_store() {
            Ok(store) => store,
            Err(_) => return AuthorizationStatus::Denied,
        };
        let _ = store.request_full_access_to_reminders();
        self.authorization_status()
    }

    fn list(&self) -> Result<Vec<Reminder>, DeviceError> {
        let store = open_store()?;
        let found = store
            .fetch_reminders_matching(&EKReminderPredicate::new())
            .map_err(map_error)?;
        Ok(found
            .into_iter()
            .filter_map(|item| {
                item.identifier.map(|id| Reminder {
                    id,
                    title: item.title,
                    completed: item.is_completed,
                })
            })
            .collect())
    }

    fn get(&self, id: &str) -> Result<Option<Reminder>, DeviceError> {
        Ok(self.list()?.into_iter().find(|item| item.id == id))
    }

    fn set_completed(&self, id: &str, completed: bool) -> Result<Reminder, DeviceError> {
        // 未找到时不写。提交返回只说明保存调用成功；execute_set 会重新
        // 查询实际状态后才记录完成，不以此处本地对象证明落地或断电耐久性。
        let store = open_store()?;
        let found = store
            .fetch_reminders_matching(&EKReminderPredicate::new())
            .map_err(map_error)?;
        let mut item = found
            .into_iter()
            .find(|item| item.identifier.as_deref() == Some(id))
            .ok_or_else(|| DeviceError::new(DeviceErrorKind::NotFound, "reminder not found"))?;
        prepare_completed(&mut item, completed);
        store.save_reminder(&item, true).map_err(map_error)?;
        Ok(Reminder {
            id: item.identifier.unwrap_or_default(),
            title: item.title,
            completed: item.is_completed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_state_clears_stale_completion_date() {
        for target in [false, true] {
            let mut item = EKReminder::new("test");
            item.is_completed = true;
            item.completion_date = Some("2026-10-25T00:00:00Z".into());
            prepare_completed(&mut item, target);
            let payload = serde_json::to_value(&item).unwrap();
            assert!(payload["completionDate"].is_null());
            assert_eq!(payload["isCompleted"], target);
        }
    }
}

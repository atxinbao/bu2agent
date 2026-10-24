//! PermissionSnapshot, precheck and degrade.
//!
//! Every capability call starts from a snapshot. A denied
//! capability degrades with a retryable, actionable prompt --
//! it never pretends to succeed.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemStatus {
    Granted,
    Denied,
    NotDetermined,
    NotSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionSnapshot {
    pub capability: String,
    pub system_status: SystemStatus,
    pub scope: Vec<String>,
    pub source: String,
    pub updated_at: String, // ISO 8601
}

impl PermissionSnapshot {
    pub fn can_proceed(&self) -> bool {
        self.system_status == SystemStatus::Granted
    }
}

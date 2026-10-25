//! HatchTools registry skeleton.
//!
//! Tools are registered in Rust, gated by permission and
//! approval before execution. Phase 1 registers file tools
//! only. Phase 4 adds the Mock connector tools (notes.list,
//! notes.create, mail.list, mail.send) -- Mock only, fake
//! data, never real accounts; real mail / messages / notes /
//! browser / screen / dictation tools are still deferred,
//! not stubbed as working. Reminders reads were verified on
//! 2026-10-25; real reminders writes stay deferred pending
//! EventKit verification (the gateway device set is Fake only).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub requires_approval: bool,
    pub capability: String,
}

#[derive(Debug, Clone, Default)]
pub struct ToolRegistry {
    pub tools: Vec<ToolSpec>,
}

impl ToolRegistry {
    pub fn phase1() -> Self {
        Self {
            tools: vec![
                ToolSpec { name: "fs.list".to_string(), requires_approval: false, capability: "files".to_string() },
                ToolSpec { name: "fs.read".to_string(), requires_approval: false, capability: "files".to_string() },
                ToolSpec { name: "fs.write".to_string(), requires_approval: true, capability: "files".to_string() },
                ToolSpec { name: "fs.delete".to_string(), requires_approval: true, capability: "files".to_string() },
            ],
        }
    }

    /// Phase 4 registry: Phase 1 file tools plus the Mock
    /// connector tools. Approval flags mirror the server's
    /// `tool_requires_approval` in mock-gateway lib.rs.
    pub fn phase4() -> Self {
        let mut registry = Self::phase1();
        registry.tools.push(ToolSpec { name: "notes.list".to_string(), requires_approval: false, capability: "notes".to_string() });
        registry.tools.push(ToolSpec { name: "notes.create".to_string(), requires_approval: true, capability: "notes".to_string() });
        registry.tools.push(ToolSpec { name: "mail.list".to_string(), requires_approval: false, capability: "mail".to_string() });
        registry.tools.push(ToolSpec { name: "mail.send".to_string(), requires_approval: true, capability: "mail".to_string() });
        registry.tools.push(ToolSpec { name: "reminders.set_completed".to_string(), requires_approval: true, capability: "reminders".to_string() });
        registry
    }

    pub fn get(&self, name: &str) -> Option<&ToolSpec> {
        self.tools.iter().find(|t| t.name == name)
    }
}

/// Connection status as verified on the user's Mac (2026-10-24
/// baseline; reminders reads re-verified 2026-10-25).
/// This is a snapshot, not a live permission check: the Rust client
/// must re-check PermissionSnapshot at call time. Content from
/// verification probes is never stored here -- only status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolConnection {
    /// Read probe succeeded (or permission is granted for reads).
    ConnectedRead,
    /// macOS permission is not granted; needs a user consent step.
    PermissionDenied,
    /// No device command / client plugin yet; do not stub as working.
    Deferred,
    /// Available only behind a per-task approval, never unrestricted.
    ApprovalGated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolInventory {
    pub name: String,
    pub capability: String,
    pub connection: ToolConnection,
    pub requires_approval: bool,
}

/// Full HatchTools inventory. Phase 1 only *executes* file tools;
/// the rest are inventoried here so the client cannot silently
/// claim a tool it has not actually connected.
pub fn full_inventory() -> Vec<ToolInventory> {
    vec![
        ToolInventory { name: "files".to_string(), capability: "files".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: true },
        ToolInventory { name: "mail.read".to_string(), capability: "mail".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: false },
        ToolInventory { name: "mail.send".to_string(), capability: "mail".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "messages.read".to_string(), capability: "messages".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: false },
        ToolInventory { name: "messages.send".to_string(), capability: "messages".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "notes.read".to_string(), capability: "notes".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: false },
        ToolInventory { name: "notes.write".to_string(), capability: "notes".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "calendar".to_string(), capability: "calendar".to_string(), connection: ToolConnection::PermissionDenied, requires_approval: false },
        ToolInventory { name: "contacts".to_string(), capability: "contacts".to_string(), connection: ToolConnection::PermissionDenied, requires_approval: false },
        ToolInventory { name: "reminders".to_string(), capability: "reminders".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: false },
        ToolInventory { name: "reminders.write".to_string(), capability: "reminders".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "whatsapp.read".to_string(), capability: "whatsapp".to_string(), connection: ToolConnection::ConnectedRead, requires_approval: false },
        ToolInventory { name: "browser.computer_use".to_string(), capability: "browser".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "screen".to_string(), capability: "screen".to_string(), connection: ToolConnection::ApprovalGated, requires_approval: true },
        ToolInventory { name: "camera".to_string(), capability: "camera".to_string(), connection: ToolConnection::Deferred, requires_approval: true },
        ToolInventory { name: "dictation".to_string(), capability: "dictation".to_string(), connection: ToolConnection::Deferred, requires_approval: false },
        ToolInventory { name: "photos".to_string(), capability: "photos".to_string(), connection: ToolConnection::Deferred, requires_approval: false },
    ]
}

//! HatchNodeCore / HatchNodeProtocol equivalent: node registration,
//! command dispatch, history, sync, permission gating and
//! invoke result / ack. Field shapes still need runtime validation
//! against the real gateway (baseline 4.5, 12).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMessage {
    pub client_id: String,
    pub invoke_id: String,
    pub command: String,
    pub args: String, // JSON text
    pub card_json: String, // JSON text
    pub display_name: String,
    pub device_metadata: String, // JSON text
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvokeOutcome {
    Result(String), // JSON text
    Ack,
}

/// Agent turn state machine (baseline 5.2). Shared by the
/// Rust shell and the TS Domain Store; neither side may invent
/// extra terminal states without a contract change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Created,
    Submitted,
    Thinking,
    ToolCallPending,
    AwaitingApproval,
    ToolRunning,
    Streaming,
    Completed,
    Cancelled,
    Failed,
}

impl TurnState {
    pub fn is_terminal(self) -> bool {
        matches!(self, TurnState::Completed | TurnState::Cancelled | TurnState::Failed)
    }
}

/// Chat stream event names (baseline 5.2).
pub mod events {
    pub const MESSAGE_START: &str = "delta.message_start";
    pub const PLACEHOLDER: &str = "delta.placeholder";
    pub const TEXT_APPEND: &str = "delta.text_append";
    pub const TURN_START: &str = "delta.turn_start";
    pub const AGENT_STATUS: &str = "delta.agent_status";
    pub const MESSAGE_DONE: &str = "delta.message_done";
    pub const MESSAGE_ASSISTANT: &str = "message.assistant";
    pub const APPLY_EVENT: &str = "stream/apply_event";
}

//! Node registry records (Task 19, node record subset).
//!
//! A node is a registered peer device: register, list,
//! heartbeat, disconnect, and unpair. Invoke-ack,
//! invoke-result, and sync are excluded (see the Task 19
//! plan): they are receipt/sync protocol with a real node,
//! and there is no local counterpart.
//!
//! Persistence mirrors the session store: one JSON object
//! per line, every mutation appends a full snapshot, and
//! the last line per id wins on reload. Unpair is a
//! tombstone, not erasure: an unpaired node is hidden from
//! list and reads as unknown to heartbeat/disconnect, but
//! its earlier snapshot lines stay in the file until the
//! file itself is removed.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Online, or explicitly disconnected (heartbeat brings a
/// disconnected node back online; see D3/D4 of the plan).
pub const NODE_ONLINE: &str = "online";
pub const NODE_DISCONNECTED: &str = "disconnected";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub name: String,
    pub status: String,
    pub last_heartbeat_ms: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub unpaired: bool,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next node number after the loaded records, mirroring
/// the peer store's numbering helpers.
pub fn next_node_number(nodes: &[Node]) -> u64 {
    nodes.iter().map(|n| numeric_suffix(&n.node_id, "node-")).max().unwrap_or(0) + 1
}

/// Load node snapshots; malformed lines are skipped with a
/// warning, mirroring the other stores. Last line per id
/// wins.
pub fn load_nodes(path: &Path) -> Vec<Node> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut nodes: Vec<Node> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Node>(line) {
            Ok(record) => {
                if let Some(existing) = nodes.iter_mut().find(|n| n.node_id == record.node_id) {
                    *existing = record;
                } else {
                    nodes.push(record);
                }
            }
            Err(_) => eprintln!("warning: skipping malformed node state line in {}", path.display()),
        }
    }
    nodes
}

/// Append one node snapshot; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_node(path: &Path, record: &Node) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append node state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append node state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> Node {
        Node {
            node_id: id.to_string(),
            name: id.to_string(),
            status: NODE_ONLINE.to_string(),
            last_heartbeat_ms: 0,
            created_ms: 0,
            updated_ms: 0,
            unpaired: false,
        }
    }

    #[test]
    fn next_node_number_follows_max_suffix() {
        assert_eq!(next_node_number(&[]), 1);
        assert_eq!(next_node_number(&[node("node-2"), node("node-7")]), 8);
    }
}

import { invoke } from "@tauri-apps/api/core";
import type { Node, NodeGateway } from "./node_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

interface ServerNode {
  node_id: string;
  name: string;
  status: string;
  last_heartbeat_ms: number;
  created_ms: number;
  updated_ms: number;
  unpaired?: boolean;
}

function toNode(server: ServerNode): Node {
  return {
    nodeId: server.node_id,
    name: server.name,
    status: server.status,
    lastHeartbeatMs: server.last_heartbeat_ms,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    unpaired: server.unpaired ?? false,
  };
}

export class InvokeNodeClient implements NodeGateway {
  async registerNode(name: string | null): Promise<Node> {
    return toNode(await invoke<ServerNode>("node_register", { name }));
  }

  async listNodes(status: string | null): Promise<Node[]> {
    return (await invoke<ServerNode[]>("node_list", { status })).map(toNode);
  }

  async heartbeatNode(nodeId: string): Promise<Node> {
    return toNode(await invoke<ServerNode>("node_heartbeat", { node_id: nodeId }));
  }

  async disconnectNode(nodeId: string): Promise<Node> {
    return toNode(await invoke<ServerNode>("node_disconnect", { node_id: nodeId }));
  }

  async unpairNode(nodeId: string): Promise<Node> {
    return toNode(await invoke<ServerNode>("node_unpair", { node_id: nodeId }));
  }
}

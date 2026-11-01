// Node client for the TS Domain Store (Task 19, node
// record subset): register, list, heartbeat, disconnect,
// unpair.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface Node {
  nodeId: string;
  name: string;
  status: string;
  lastHeartbeatMs: number;
  createdMs: number;
  updatedMs: number;
  unpaired: boolean;
}

export interface NodeGateway {
  registerNode(name: string | null): Promise<Node>;
  listNodes(status: string | null): Promise<Node[]>;
  heartbeatNode(nodeId: string): Promise<Node>;
  disconnectNode(nodeId: string): Promise<Node>;
  unpairNode(nodeId: string): Promise<Node>;
}

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

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class NodeClient implements NodeGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async registerNode(name: string | null): Promise<Node> {
    return toNode(await this.request<ServerNode>("POST", "/api/nodes/register", { name }));
  }

  async listNodes(status: string | null): Promise<Node[]> {
    return (await this.request<ServerNode[]>("POST", "/api/nodes/list", { status })).map(toNode);
  }

  async heartbeatNode(nodeId: string): Promise<Node> {
    return toNode(
      await this.request<ServerNode>("POST", "/api/nodes/heartbeat", { node_id: nodeId }),
    );
  }

  async disconnectNode(nodeId: string): Promise<Node> {
    return toNode(
      await this.request<ServerNode>("POST", "/api/nodes/disconnect", { node_id: nodeId }),
    );
  }

  async unpairNode(nodeId: string): Promise<Node> {
    return toNode(
      await this.request<ServerNode>("POST", "/api/nodes/unpair", { node_id: nodeId }),
    );
  }
}

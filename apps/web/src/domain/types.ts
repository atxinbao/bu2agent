// TS contract types. Must stay aligned with
// packages/contract/schemas/gateway-contract.schema.json and
// packages/contract/schemas/bridge-contract.schema.json.
// A field rename here without a schema change is a contract break.

export type GatewayService =
  | "gateway"
  | "daemon"
  | "authd"
  | "sentinel"
  | "vault"
  | "noise-only"
  | "media";

export interface GatewayTargetKey {
  vmName: string;
  gatewayUrl: string;
  vmId?: string;
  generation: number;
}

export interface GatewayRequest {
  requestId: string;
  method: string;
  path: string;
  service: GatewayService;
  noiseOnly: boolean;
  timeoutMs: number;
  targetKey: GatewayTargetKey;
  body?: unknown;
}

export interface SubscriptionRecord {
  streamId: string;
  sequence: number;
  eventType: string;
  payload: unknown;
  resumeCursor: string | null;
}

export type ApprovalStatus =
  | "not_required"
  | "pending"
  | "approved"
  | "rejected"
  | "edited";

export interface AgentInvocation {
  turnId: string;
  invocationId: string;
  tool: string;
  argsSummary: string;
  approvalStatus: ApprovalStatus;
  startedAt: string;
  endedAt: string | null;
  resultRef: string | null;
  error: UnifiedError | null;
}

export interface UnifiedError {
  code:
    | "permission_denied"
    | "not_running"
    | "not_supported"
    | "timeout"
    | "transport"
    | "gateway"
    | "invalid_input";
  capability?: string;
  retryable: boolean;
  userAction:
    | "open_system_settings"
    | "open_app"
    | "retry"
    | "request_approval"
    | "none";
  redactedDetail: string;
}

export interface PermissionSnapshot {
  capability: string;
  systemStatus: "granted" | "denied" | "not_determined" | "not_supported";
  scope: string[];
  source: string;
  updatedAt: string;
}

export interface BridgeCommand {
  action: string;
  payload: Record<string, unknown>;
  requestId: string;
  bridgeVersion: "1";
}

export interface BridgeEvent {
  event: string;
  payload: unknown;
  bridgeVersion: "1";
  requestId?: string | null;
  streamId?: string | null;
}

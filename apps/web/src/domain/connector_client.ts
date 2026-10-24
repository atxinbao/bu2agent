// Mock connector client for the TS Domain Store (Phase 4).
//
// Same rules as the Rust connector client, in the same order:
// 1. Egress: only the localhost Mock host, and mail only
//    inside the mock.local domain.
// 2. Registry: notes.list / notes.create, mail.list /
//    mail.send; anything else is rejected before a request.
// 3. Approval (Phase 3 loop, reused): notes.create and
//    mail.send need a decided approval record; the server
//    re-checks its own record before executing.
// 4. Payload: secret markers are refused before sending.
//
// All connector data is fake (Mock only). No real accounts,
// no real delivery, no OAuth. Credentials are not handled
// here at all: the Rust memory-only store is the Phase 4
// credential story, and it never leaves its process.

export interface MockNote {
  id: string;
  title: string;
  body: string;
}

export interface MockMessage {
  id: string;
  from: string;
  to: string;
  subject: string;
  body: string;
}

export type ConnectorDecision = "ask" | "approve" | "reject";

export class ApprovalPendingError extends Error {
  readonly toolCallId: string;

  constructor(toolCallId: string) {
    super(`approval required: tool_call_id=${toolCallId}`);
    this.toolCallId = toolCallId;
  }
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];
const ALLOWED_DOMAINS = ["mock.local"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

function checkRecipient(address: string): void {
  const domain = address.split("@").pop() ?? "";
  if (!address.includes("@") || !ALLOWED_DOMAINS.includes(domain)) {
    throw new Error("egress refused: recipient domain is not on the Mock allowlist");
  }
}

function checkPayload(payload: string): void {
  const lower = payload.toLowerCase();
  if (lower.includes("token:") || lower.includes("bearer ") || lower.includes("api_key=")) {
    throw new Error("egress refused: payload looks like it carries a secret");
  }
}

function toolName(action: string): string {
  return action;
}

function requiresApproval(action: string): boolean {
  return action === "notes.create" || action === "mail.send";
}

// Transport-neutral surface used by the Connectors view. The
// HTTP client below and the invoke client both implement it,
// so the view never knows which transport it runs on.
export interface ConnectorGateway {
  notesList(): Promise<MockNote[]>;
  mailList(): Promise<MockMessage[]>;
  notesCreate(title: string, body: string, decision: ConnectorDecision): Promise<MockNote>;
  mailSend(to: string, subject: string, body: string, decision: ConnectorDecision): Promise<MockMessage>;
}

export class ConnectorClient implements ConnectorGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async get<T>(path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (response.status === 403) {
      throw new Error("connector call refused: approval required or rejected");
    }
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  private async approve(action: string, decision: ConnectorDecision): Promise<string> {
    const request = await this.post<{
      tool_call_id: string | null;
      requires_approval: boolean;
      status: string;
    }>("/approval/request", { tool: toolName(action) });
    if (!request.tool_call_id) {
      throw new Error("approval tool returned no tool_call_id");
    }
    if (decision === "ask") {
      throw new ApprovalPendingError(request.tool_call_id);
    }
    const status = decision === "approve" ? "approved" : "rejected";
    await this.post("/approval/decide", { tool_call_id: request.tool_call_id, status });
    return request.tool_call_id;
  }

  async notesList(): Promise<MockNote[]> {
    return this.get<MockNote[]>("/connectors/notes/list");
  }

  async mailList(): Promise<MockMessage[]> {
    return this.get<MockMessage[]>("/connectors/mail/list");
  }

  async notesCreate(title: string, body: string, decision: ConnectorDecision): Promise<MockNote> {
    checkPayload(title);
    checkPayload(body);
    const toolCallId = requiresApproval("notes.create")
      ? await this.approve("notes.create", decision)
      : undefined;
    return this.post<MockNote>("/connectors/notes/create", { tool_call_id: toolCallId ?? null, title, body });
  }

  async mailSend(to: string, subject: string, body: string, decision: ConnectorDecision): Promise<MockMessage> {
    checkRecipient(to);
    checkPayload(subject);
    checkPayload(body);
    const toolCallId = requiresApproval("mail.send")
      ? await this.approve("mail.send", decision)
      : undefined;
    return this.post<MockMessage>("/connectors/mail/send", { tool_call_id: toolCallId ?? null, to, subject, body });
  }
}

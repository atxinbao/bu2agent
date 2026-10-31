// Feed client for the TS Domain Store (Task 17, unit
// subset): list, infinite (alias), status, unit get,
// delete, reaction, tap, seen.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface FeedUnit {
  unitId: string;
  title: string;
  body: string;
  source: string;
  seen: boolean;
  reaction: string | null;
  tapCount: number;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
}

export interface FeedStatus {
  units: number;
  unseen: number;
}

export interface FeedSeenResult {
  seen: string[];
  unknown: string[];
}

interface ServerFeedUnit {
  unit_id: string;
  title: string;
  body: string;
  source: string;
  seen: boolean;
  reaction: string | null;
  tap_count: number;
  created_ms: number;
  updated_ms: number;
  deleted?: boolean;
}

interface ServerFeedStatus {
  units: number;
  unseen: number;
}

interface ServerFeedSeenResult {
  seen: string[];
  unknown: string[];
}

function toFeedUnit(server: ServerFeedUnit): FeedUnit {
  return {
    unitId: server.unit_id,
    title: server.title,
    body: server.body,
    source: server.source,
    seen: server.seen,
    reaction: server.reaction,
    tapCount: server.tap_count,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted ?? false,
  };
}

function toFeedStatus(server: ServerFeedStatus): FeedStatus {
  return { units: server.units, unseen: server.unseen };
}

function toFeedSeenResult(server: ServerFeedSeenResult): FeedSeenResult {
  return { seen: server.seen, unknown: server.unknown };
}

// Transport-neutral surface used by the Feed view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface FeedGateway {
  listUnits(): Promise<FeedUnit[]>;
  listInfinite(): Promise<FeedUnit[]>;
  getStatus(): Promise<FeedStatus>;
  getUnit(unitId: string): Promise<FeedUnit>;
  deleteUnit(unitId: string): Promise<FeedUnit>;
  reactUnit(unitId: string, reaction: string): Promise<FeedUnit>;
  tapUnit(unitId: string): Promise<FeedUnit>;
  seenUnits(unitIds: string[]): Promise<FeedSeenResult>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class FeedClient implements FeedGateway {
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

  async listUnits(): Promise<FeedUnit[]> {
    return (await this.request<ServerFeedUnit[]>("GET", "/api/feed")).map(toFeedUnit);
  }

  async listInfinite(): Promise<FeedUnit[]> {
    return (await this.request<ServerFeedUnit[]>("GET", "/api/feed/infinite")).map(toFeedUnit);
  }

  async getStatus(): Promise<FeedStatus> {
    return toFeedStatus(await this.request<ServerFeedStatus>("GET", "/api/feed/status"));
  }

  async getUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await this.request<ServerFeedUnit>("GET", `/api/feed/units/${unitId}`));
  }

  async deleteUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await this.request<ServerFeedUnit>("DELETE", `/api/feed/units/${unitId}`));
  }

  async reactUnit(unitId: string, reaction: string): Promise<FeedUnit> {
    return toFeedUnit(
      await this.request<ServerFeedUnit>("POST", `/api/feed/units/${unitId}/reaction`, {
        reaction,
      }),
    );
  }

  async tapUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await this.request<ServerFeedUnit>("POST", `/api/feed/units/${unitId}/tap`));
  }

  async seenUnits(unitIds: string[]): Promise<FeedSeenResult> {
    return toFeedSeenResult(
      await this.request<ServerFeedSeenResult>("POST", "/api/feed/units/seen", {
        unit_ids: unitIds,
      }),
    );
  }
}

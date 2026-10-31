// Space client for the TS Domain Store (Task 16, v2
// record subset): list, save (upsert), delete.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface SpaceItem {
  kind: string;
  ref: string;
}

export interface Space {
  slug: string;
  title: string;
  items: SpaceItem[];
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
}

interface ServerSpaceItem {
  kind: string;
  ref: string;
}

interface ServerSpace {
  slug: string;
  title: string;
  items: ServerSpaceItem[];
  created_ms: number;
  updated_ms: number;
  deleted?: boolean;
}

function toSpaceItem(server: ServerSpaceItem): SpaceItem {
  return { kind: server.kind, ref: server.ref };
}

function toSpace(server: ServerSpace): Space {
  return {
    slug: server.slug,
    title: server.title,
    items: server.items.map(toSpaceItem),
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted ?? false,
  };
}

// Transport-neutral surface used by the Spaces view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface SpaceGateway {
  listSpaces(): Promise<Space[]>;
  saveSpace(slug: string, title: string | undefined, items: SpaceItem[]): Promise<Space>;
  deleteSpace(slug: string): Promise<Space>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class SpaceClient implements SpaceGateway {
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

  async listSpaces(): Promise<Space[]> {
    return (await this.request<ServerSpace[]>("GET", "/spaces/v2")).map(toSpace);
  }

  async saveSpace(slug: string, title: string | undefined, items: SpaceItem[]): Promise<Space> {
    return toSpace(
      await this.request<ServerSpace>("POST", `/spaces/v2/${slug}/save`, { title, items }),
    );
  }

  async deleteSpace(slug: string): Promise<Space> {
    return toSpace(await this.request<ServerSpace>("DELETE", `/spaces/v2/${slug}`));
  }
}

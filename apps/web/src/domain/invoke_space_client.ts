import { invoke } from "@tauri-apps/api/core";
import type { Space, SpaceGateway, SpaceItem } from "./space_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

function toSpace(server: ServerSpace): Space {
  return {
    slug: server.slug,
    title: server.title,
    items: server.items.map((item) => ({ kind: item.kind, ref: item.ref })),
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted ?? false,
  };
}

export class InvokeSpaceClient implements SpaceGateway {
  async listSpaces(): Promise<Space[]> {
    return (await invoke<ServerSpace[]>("space_list")).map(toSpace);
  }

  async saveSpace(slug: string, title: string | undefined, items: SpaceItem[]): Promise<Space> {
    return toSpace(
      await invoke<ServerSpace>("space_save", { slug, title, items }),
    );
  }

  async deleteSpace(slug: string): Promise<Space> {
    return toSpace(await invoke<ServerSpace>("space_delete", { slug }));
  }
}

import { invoke } from "@tauri-apps/api/core";
import type {
  FeedGateway,
  FeedSeenResult,
  FeedStatus,
  FeedUnit,
} from "./feed_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

export class InvokeFeedClient implements FeedGateway {
  async listUnits(): Promise<FeedUnit[]> {
    return (await invoke<ServerFeedUnit[]>("feed_list")).map(toFeedUnit);
  }

  // The invoke transport has no infinite-scroll alias: the
  // gateway's infinite route returns the same list, so
  // this reuses the list command.
  async listInfinite(): Promise<FeedUnit[]> {
    return this.listUnits();
  }

  async getStatus(): Promise<FeedStatus> {
    const status = await invoke<ServerFeedStatus>("feed_status");
    return { units: status.units, unseen: status.unseen };
  }

  async getUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await invoke<ServerFeedUnit>("feed_get", { unit_id: unitId }));
  }

  async deleteUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await invoke<ServerFeedUnit>("feed_delete", { unit_id: unitId }));
  }

  async reactUnit(unitId: string, reaction: string): Promise<FeedUnit> {
    return toFeedUnit(
      await invoke<ServerFeedUnit>("feed_react", { unit_id: unitId, reaction }),
    );
  }

  async tapUnit(unitId: string): Promise<FeedUnit> {
    return toFeedUnit(await invoke<ServerFeedUnit>("feed_tap", { unit_id: unitId }));
  }

  async seenUnits(unitIds: string[]): Promise<FeedSeenResult> {
    const result = await invoke<ServerFeedSeenResult>("feed_seen", { unit_ids: unitIds });
    return { seen: result.seen, unknown: result.unknown };
  }
}

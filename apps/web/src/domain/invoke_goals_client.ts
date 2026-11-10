import { invoke } from "@tauri-apps/api/core";
import type { Goal, GoalGateway } from "./goals_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

interface ServerGoal {
  goal_id: string;
  title: string;
  description: string;
  status: string;
  created_ms: number;
  updated_ms: number;
  origin_device_id: string;
  schema_version: number;
}

function toGoal(server: ServerGoal): Goal {
  return {
    goalId: server.goal_id,
    title: server.title,
    description: server.description,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
  };
}

export class InvokeGoalClient implements GoalGateway {
  async listGoals(): Promise<Goal[]> {
    return (await invoke<ServerGoal[]>("goal_list")).map(toGoal);
  }

  async getGoal(goalId: string): Promise<Goal> {
    return toGoal(await invoke<ServerGoal>("goal_get", { goal_id: goalId }));
  }

  async createGoal(title: string, description: string | undefined): Promise<Goal> {
    return toGoal(await invoke<ServerGoal>("goal_create", { title, description }));
  }

  async archiveGoal(goalId: string): Promise<Goal> {
    return toGoal(await invoke<ServerGoal>("goal_archive", { goal_id: goalId }));
  }
}

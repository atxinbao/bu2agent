// Invoke swarm client: packaged app -> Tauri shell
// swarm_* commands (ORC-5 create + fanout + read).
// Param names are snake_case to match the shell's
// rename_all; the invoke transport returns the Rust
// structs' snake_case fields, so the mapping lives
// here, not in the view.
//
// The client never requests or decides an approval:
// the step approvals are built gateway-side by the
// sweeper drive, and the user decides them through
// the existing approval surface.

import { invoke } from "@tauri-apps/api/core";
import type {
  Swarm,
  SwarmFanoutInput,
  SwarmGateway,
} from "./swarm_client";
import { toSwarm } from "./swarm_client";

interface ServerSwarmStepView {
  step_no: number;
  worker_id: string;
  worktree_id?: string | null;
  run_id?: string | null;
  run_status: string;
  approval_id?: string | null;
  approval_status?: string | null;
}

interface ServerSwarmMessageView {
  message_id: string;
  sender_worker_id: string;
  kind: string;
  body_hash: string;
  created_ms: number;
}

interface ServerSwarm {
  swarm_id: string;
  task_id: string;
  plan_id: string;
  status: string;
  steps: ServerSwarmStepView[];
  messages: ServerSwarmMessageView[];
  leftover_worktrees: string[];
  created_ms: number;
  updated_ms: number;
}

export class InvokeSwarmClient implements SwarmGateway {
  async createSwarm(taskId: string): Promise<Swarm> {
    const server = await invoke<ServerSwarm>("swarm_create", {
      task_id: taskId,
    });
    return toSwarm(server);
  }

  async fanoutSwarm(swarmId: string, input: SwarmFanoutInput): Promise<Swarm> {
    const server = await invoke<ServerSwarm>("swarm_fanout", {
      swarm_id: swarmId,
      step_nos: input.stepNos,
      priority: input.priority,
    });
    return toSwarm(server);
  }

  async swarm(swarmId: string): Promise<Swarm> {
    const server = await invoke<ServerSwarm>("swarm_get", {
      swarm_id: swarmId,
    });
    return toSwarm(server);
  }
}

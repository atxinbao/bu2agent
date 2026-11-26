// Invoke timers client: packaged app -> Tauri shell
// timer_* commands (AO-2 create + list + revoke).
// Param names are snake_case to match the shell's
// rename_all; the invoke transport returns the Rust
// structs' snake_case fields, so the mapping lives
// here, not in the view.
//
// A timer is a fact row, not an Agent run: creating
// one only records when it should fire. The sweeper
// drive fires due rows by writing an event through
// AO-1's ingest path — a timer NEVER starts an Agent
// directly.

import { invoke } from "@tauri-apps/api/core";
import type {
  Timer,
  TimerCreateInput,
  TimersGateway,
} from "./timers_client";
import { toTimer } from "./timers_client";

interface ServerTimer {
  timer_id: string;
  kind: string;
  payload_summary_json: Record<string, unknown>;
  session_id?: string | null;
  status: string;
  next_fire_ms: number;
  recurrence_json?: { interval_ms: number } | null;
  created_ms: number;
}

export class InvokeTimersClient implements TimersGateway {
  async createTimer(input: TimerCreateInput): Promise<Timer> {
    const server = await invoke<ServerTimer>("timer_create", {
      kind: input.kind,
      next_fire_ms: input.nextFireMs,
      recurrence_json: input.recurrenceJson,
      payload_summary_json: input.payloadSummaryJson,
      session_id: input.sessionId,
    });
    return toTimer(server);
  }

  async listTimers(): Promise<Timer[]> {
    const server = await invoke<ServerTimer[]>("timer_list", {
      limit: null,
    });
    return server.map(toTimer);
  }

  async deleteTimer(timerId: string): Promise<Timer> {
    const server = await invoke<ServerTimer>("timer_delete", {
      timer_id: timerId,
    });
    return toTimer(server);
  }
}

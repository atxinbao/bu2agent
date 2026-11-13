// Invoke calendar client: packaged app -> Tauri shell
// calendar_* commands (D1-6a, read-only). Param names
// are snake_case to match the shell's rename_all; the
// invoke transport returns the Rust structs' snake_case
// fields, so the mapping lives here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  CalendarEvent,
  CalendarGateway,
  CreateCalendarEventInput,
  CreatedCalendarEvent,
} from "./calendar_client";

interface ServerCalendarEvent {
  event_id: string;
  title: string;
  start_ms: number;
  end_ms: number;
  all_day: boolean;
  calendar_id: string | null;
  calendar_title: string | null;
  location: string | null;
  notes: string | null;
}

function toCalendarEvent(server: ServerCalendarEvent): CalendarEvent {
  return {
    eventId: server.event_id,
    title: server.title,
    startMs: server.start_ms,
    endMs: server.end_ms,
    allDay: server.all_day,
    calendarId: server.calendar_id,
    calendarTitle: server.calendar_title,
    location: server.location,
    notes: server.notes,
  };
}

interface ServerCreatedCalendarEvent {
  event_id: string;
  source_id: string;
  calendar_id: string;
  title: string;
  start_ms: number;
  end_ms: number;
  all_day: boolean;
  location: string | null;
  notes_hash: string;
  idempotency_key: string;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  origin_device_id: string;
}

function toCreatedCalendarEvent(server: ServerCreatedCalendarEvent): CreatedCalendarEvent {
  return {
    eventId: server.event_id,
    sourceId: server.source_id,
    calendarId: server.calendar_id,
    title: server.title,
    startMs: server.start_ms,
    endMs: server.end_ms,
    allDay: server.all_day,
    location: server.location,
    notesHash: server.notes_hash,
    idempotencyKey: server.idempotency_key,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

export class InvokeCalendarClient implements CalendarGateway {
  async listEvents(): Promise<CalendarEvent[]> {
    const events = await invoke<ServerCalendarEvent[]>("calendar_list");
    return events.map(toCalendarEvent);
  }

  async getEvent(eventId: string): Promise<CalendarEvent> {
    const event = await invoke<ServerCalendarEvent>("calendar_get", {
      event_id: eventId,
    });
    return toCalendarEvent(event);
  }

  async createEvent(input: CreateCalendarEventInput): Promise<CreatedCalendarEvent> {
    const row = await invoke<ServerCreatedCalendarEvent>("calendar_create", {
      calendar_id: input.calendarId,
      title: input.title,
      start_ms: input.startMs,
      end_ms: input.endMs,
      all_day: input.allDay,
      location: input.location ?? null,
      notes: input.notes ?? null,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toCreatedCalendarEvent(row);
  }
}

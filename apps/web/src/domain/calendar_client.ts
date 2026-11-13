// Calendar client: browser/dev -> Mock Gateway calendar
// surface (D1-6a read-only, D1-6b-2 create).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only
// need the gateway; create goes through the digest-bound
// approval flow (approval_request -> decide -> create)
// and carries a client-supplied idempotency key. The
// list window is the gateway's (7 days back to 90 days
// ahead, all calendars).

export interface CalendarEvent {
  eventId: string;
  title: string;
  startMs: number;
  endMs: number;
  allDay: boolean;
  calendarId: string | null;
  calendarTitle: string | null;
  location: string | null;
  notes: string | null;
}

// D1-6b-2: a create request. The idempotency key is
// client-supplied and required (Q-D1-6b-05); the
// approval id is the decided digest-bound approval for
// this exact event (Q-D1-6b-22).
export interface CreateCalendarEventInput {
  calendarId: string;
  title: string;
  startMs: number;
  endMs: number;
  allDay: boolean;
  location?: string | null;
  notes?: string | null;
  idempotencyKey: string;
  toolCallId?: string | null;
}

// D1-6b-2: one accepted create (the `calendar_events`
// fact row). The notes body is never here — only its
// hash. The gateway's extra `replayed` flag is ignored
// on decode; the row is the fact record.
export interface CreatedCalendarEvent {
  eventId: string;
  sourceId: string;
  calendarId: string;
  title: string;
  startMs: number;
  endMs: number;
  allDay: boolean;
  location: string | null;
  notesHash: string;
  idempotencyKey: string;
  effectId: string;
  policyVersion: string;
  createdMs: number;
  originDeviceId: string;
}

// Transport-neutral surface used by the Calendar view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface CalendarGateway {
  listEvents(): Promise<CalendarEvent[]>;
  getEvent(eventId: string): Promise<CalendarEvent>;
  createEvent(input: CreateCalendarEventInput): Promise<CreatedCalendarEvent>;
}

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

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("calendar gateway URL must be a localhost Mock");
  }
}

export class CalendarClient implements CalendarGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`calendar gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`calendar gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async listEvents(): Promise<CalendarEvent[]> {
    const events = await this.requestJson<ServerCalendarEvent[]>(
      "GET",
      "/api/calendar/events",
    );
    return events.map(toCalendarEvent);
  }

  async getEvent(eventId: string): Promise<CalendarEvent> {
    const event = await this.requestJson<ServerCalendarEvent>(
      "GET",
      `/api/calendar/events/${eventId}`,
    );
    return toCalendarEvent(event);
  }

  async createEvent(input: CreateCalendarEventInput): Promise<CreatedCalendarEvent> {
    const row = await this.postJson<ServerCreatedCalendarEvent>("/api/calendar/events", {
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

// Calendar client: browser/dev -> Mock Gateway calendar
// read-only surface (D1-6a, EventKit on macOS, 501
// elsewhere).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only;
// no approval loop. The list window is the gateway's
// (7 days back to 90 days ahead, all calendars).

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

// Transport-neutral surface used by the Calendar view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface CalendarGateway {
  listEvents(): Promise<CalendarEvent[]>;
  getEvent(eventId: string): Promise<CalendarEvent>;
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
}

import { useEffect, useState } from "react";
import type { CalendarEvent, CalendarGateway } from "../domain/calendar_client";

// Calendar view over the CalendarGateway interface
// (D1-6a, read-only list/detail). The gateway reads
// EventKit live — 7 days back to 90 days ahead, all
// calendars — with no sync table and no writes. On
// non-macOS the gateway answers 501 and the error
// surfaces here unchanged; the packaged Mac app reads
// real Calendar data (first use requests full access in
// the gateway).
export function CalendarView({ client }: { client: CalendarGateway }) {
  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [selected, setSelected] = useState<CalendarEvent | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setEvents(await client.listEvents());
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function select(eventId: string) {
    setBusy(true);
    setError(null);
    try {
      setSelected(await client.getEvent(eventId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "get failed");
    } finally {
      setBusy(false);
    }
  }

  function formatTime(ms: number): string {
    if (ms === 0) return "unknown time";
    return new Date(ms).toLocaleString();
  }

  return (
    <div>
      <h2>Calendar</h2>
      <p className="note">
        日历只读：实时读取系统日历（前后 7 天到 90 天，全部日历），不写、不建同步表。非 macOS 上网关返回 501，不伪造数据。
      </p>
      {error !== null && <p className="error">{error}</p>}
      <div>
        <button type="button" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
      </div>
      {events.length === 0 ? (
        <p className="empty">No events in the window.</p>
      ) : (
        <ul>
          {events.map((event) => (
            <li key={event.eventId}>
              <button
                type="button"
                disabled={busy}
                onClick={() => void select(event.eventId)}
              >
                {event.title} ({formatTime(event.startMs)}
                {event.allDay ? ", all day" : ""})
              </button>
            </li>
          ))}
        </ul>
      )}
      {selected !== null && (
        <div>
          <h3>{selected.title}</h3>
          <p>
            {formatTime(selected.startMs)} – {formatTime(selected.endMs)}
            {selected.allDay ? " · all day" : ""}
          </p>
          <p>
            {selected.calendarTitle ?? "unknown calendar"}
            {selected.calendarId !== null ? ` (${selected.calendarId})` : ""}
          </p>
          {selected.location !== null && <p>location {selected.location}</p>}
          {selected.notes !== null && <p>notes {selected.notes}</p>}
          <p className="note">{selected.eventId}</p>
        </div>
      )}
    </div>
  );
}

import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  CalendarEvent,
  CalendarGateway,
  CreateCalendarEventInput,
  CreatedCalendarEvent,
} from "../domain/calendar_client";

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `key-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

interface PendingCreate {
  calendarId: string;
  title: string;
  startMs: number;
  endMs: number;
  allDay: boolean;
  location: string;
  notes: string;
  idempotencyKey: string;
  toolCallId: string;
}

// Calendar view over the CalendarGateway interface
// (D1-6a list/detail, D1-6b-2 create). The gateway
// reads EventKit live — 7 days back to 90 days ahead,
// all calendars — with no sync table. Create goes
// through the digest-bound approval flow
// (approval_request -> decide -> create) and writes
// one `calendar_events` fact row; the caller names the
// calendar id (missing -> 400, unknown -> 404, never a
// silent default). On non-macOS the gateway answers
// 501 and the error surfaces here unchanged; the
// packaged Mac app reads real Calendar data (first use
// requests full access in the gateway).
export function CalendarView({
  client,
  agent,
}: {
  client: CalendarGateway;
  agent: AgentGateway;
}) {
  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [selected, setSelected] = useState<CalendarEvent | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createResult, setCreateResult] = useState<CreatedCalendarEvent | null>(null);
  const [calendarIdInput, setCalendarIdInput] = useState("");
  const [titleInput, setTitleInput] = useState("");
  const [startInput, setStartInput] = useState("");
  const [endInput, setEndInput] = useState("");
  const [allDayInput, setAllDayInput] = useState(false);
  const [locationInput, setLocationInput] = useState("");
  const [notesInput, setNotesInput] = useState("");
  const [pendingCreate, setPendingCreate] = useState<PendingCreate | null>(null);

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

  // D1-6b-2 create flow: request a digest-bound approval
  // for this exact event, then decide in this view, then
  // create with the decided approval id. The pending copy
  // is frozen at request time: editing the form afterwards
  // cannot change what the approval bound.
  async function requestCreateApproval() {
    const startMs = new Date(startInput).getTime();
    const endMs = new Date(endInput).getTime();
    if (calendarIdInput === "" || titleInput === "") {
      setError("日历 ID、标题不能为空。");
      return;
    }
    if (Number.isNaN(startMs) || Number.isNaN(endMs) || endMs <= startMs) {
      setError("开始、结束时间无效，结束必须晚于开始。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const args: Record<string, string> = {
        calendar_id: calendarIdInput,
        title: titleInput,
        start_ms: String(startMs),
        end_ms: String(endMs),
        all_day: String(allDayInput),
      };
      if (locationInput !== "") args.location = locationInput;
      if (notesInput !== "") args.notes = notesInput;
      const request = await agent.requestApproval("calendar.create", args);
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingCreate({
        calendarId: calendarIdInput,
        title: titleInput,
        startMs,
        endMs,
        allDay: allDayInput,
        location: locationInput,
        notes: notesInput,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndCreate() {
    if (pendingCreate === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingCreate.toolCallId, "approve");
      const input: CreateCalendarEventInput = {
        calendarId: pendingCreate.calendarId,
        title: pendingCreate.title,
        startMs: pendingCreate.startMs,
        endMs: pendingCreate.endMs,
        allDay: pendingCreate.allDay,
        location: pendingCreate.location === "" ? null : pendingCreate.location,
        notes: pendingCreate.notes === "" ? null : pendingCreate.notes,
        idempotencyKey: pendingCreate.idempotencyKey,
        toolCallId: pendingCreate.toolCallId,
      };
      const row = await client.createEvent(input);
      setCreateResult(row);
      setPendingCreate(null);
      setTitleInput("");
      setStartInput("");
      setEndInput("");
      setLocationInput("");
      setNotesInput("");
      await refresh();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function rejectCreate() {
    if (pendingCreate === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingCreate.toolCallId, "reject");
      setPendingCreate(null);
    } catch (err) {
      setError(errText(err));
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
        日历：实时读取系统日历（前后 7 天到 90 天，全部日历），不建同步表。新建走审批流（先批再建），须填已有日历的 ID；非 macOS 上网关返回 501，不伪造数据。
      </p>
      {error !== null && <p className="error">{error}</p>}
      <div>
        <button type="button" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
      </div>
      <div>
        <h3>新建事件</h3>
        <label>
          日历 ID
          <input
            type="text"
            value={calendarIdInput}
            disabled={busy}
            onChange={(e) => setCalendarIdInput(e.target.value)}
          />
        </label>
        <label>
          标题
          <input
            type="text"
            value={titleInput}
            disabled={busy}
            onChange={(e) => setTitleInput(e.target.value)}
          />
        </label>
        <label>
          开始
          <input
            type="datetime-local"
            value={startInput}
            disabled={busy}
            onChange={(e) => setStartInput(e.target.value)}
          />
        </label>
        <label>
          结束
          <input
            type="datetime-local"
            value={endInput}
            disabled={busy}
            onChange={(e) => setEndInput(e.target.value)}
          />
        </label>
        <label>
          全天
          <input
            type="checkbox"
            checked={allDayInput}
            disabled={busy}
            onChange={(e) => setAllDayInput(e.target.checked)}
          />
        </label>
        <label>
          地点
          <input
            type="text"
            value={locationInput}
            disabled={busy}
            onChange={(e) => setLocationInput(e.target.value)}
          />
        </label>
        <label>
          备注
          <input
            type="text"
            value={notesInput}
            disabled={busy}
            onChange={(e) => setNotesInput(e.target.value)}
          />
        </label>
        <button type="button" disabled={busy} onClick={() => void requestCreateApproval()}>
          请求审批并新建
        </button>
      </div>
      {pendingCreate !== null && (
        <div>
          <p>
            待审批新建：{pendingCreate.title}（{formatTime(pendingCreate.startMs)} –{" "}
            {formatTime(pendingCreate.endMs)}，日历 {pendingCreate.calendarId}）
          </p>
          <button type="button" disabled={busy} onClick={() => void approveAndCreate()}>
            批准并新建
          </button>
          <button type="button" disabled={busy} onClick={() => void rejectCreate()}>
            拒绝
          </button>
        </div>
      )}
      {createResult !== null && (
        <p>
          已新建：{createResult.title}（{createResult.eventId}）
        </p>
      )}
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

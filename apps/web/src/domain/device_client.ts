// Device action client for the TS Domain Store (B-layer wiring).
//
// Same shape as the gateway device endpoints, in the same order:
// list reminders, create an action, execute a set, record a
// rejection, refresh an unknown action, list/get actions.
// The gateway persists records to JSONL when
// MOCK_DEVICE_ACTIONS_PATH is set; otherwise they live in
// memory only.
//
// Device action statuses, from the design doc: requested ->
// approved -> completed | rejected | failed | unknown |
// awaiting_authorization. The client does not enforce the
// transitions; the view drives them step by step.

export interface DeviceReminder {
  id: string;
  title: string;
  completed: boolean;
}

export interface DeviceActionRecord {
  actionId: string;
  device: string;
  action: string;
  reminderId: string;
  targetCompleted: boolean;
  status: string;
  detail: string | null;
}

interface ServerDeviceActionRecord {
  action_id: string;
  device: string;
  action: string;
  reminder_id: string;
  target_completed: boolean;
  status: string;
  detail: string | null;
}

function toRecord(server: ServerDeviceActionRecord): DeviceActionRecord {
  return {
    actionId: server.action_id,
    device: server.device,
    action: server.action,
    reminderId: server.reminder_id,
    targetCompleted: server.target_completed,
    status: server.status,
    detail: server.detail,
  };
}

// Transport-neutral surface used by the Devices view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface DeviceGateway {
  listReminders(): Promise<DeviceReminder[]>;
  createAction(
    reminderId: string,
    targetCompleted: boolean,
  ): Promise<DeviceActionRecord>;
  setReminder(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord>;
  markRejected(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord>;
  refreshAction(actionId: string): Promise<DeviceActionRecord>;
  listActions(): Promise<DeviceActionRecord[]>;
  getAction(actionId: string): Promise<DeviceActionRecord>;
}

export class DeviceClient implements DeviceGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async listReminders(): Promise<DeviceReminder[]> {
    return this.request<DeviceReminder[]>("/devices/reminders/list");
  }

  async createAction(
    reminderId: string,
    targetCompleted: boolean,
  ): Promise<DeviceActionRecord> {
    const record = await this.request<ServerDeviceActionRecord>(
      "/devices/actions/create",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reminder_id: reminderId,
          target_completed: targetCompleted,
        }),
      },
    );
    return toRecord(record);
  }

  async setReminder(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord> {
    const record = await this.request<ServerDeviceActionRecord>(
      "/devices/reminders/set",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action_id: actionId,
          tool_call_id: toolCallId,
        }),
      },
    );
    return toRecord(record);
  }

  async markRejected(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord> {
    const record = await this.request<ServerDeviceActionRecord>(
      "/devices/actions/mark_rejected",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action_id: actionId,
          tool_call_id: toolCallId,
        }),
      },
    );
    return toRecord(record);
  }

  async refreshAction(actionId: string): Promise<DeviceActionRecord> {
    const record = await this.request<ServerDeviceActionRecord>(
      "/devices/actions/refresh",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action_id: actionId }),
      },
    );
    return toRecord(record);
  }

  async listActions(): Promise<DeviceActionRecord[]> {
    const records =
      await this.request<ServerDeviceActionRecord[]>("/devices/actions/list");
    return records.map(toRecord);
  }

  async getAction(actionId: string): Promise<DeviceActionRecord> {
    const record = await this.request<ServerDeviceActionRecord>(
      `/devices/actions/get?action_id=${actionId}`,
    );
    return toRecord(record);
  }
}

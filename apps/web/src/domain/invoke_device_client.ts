import { invoke } from "@tauri-apps/api/core";
import type {
  DeviceActionRecord,
  DeviceGateway,
  DeviceReminder,
} from "./device_client";

// The Rust device record serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
// Reminders already arrive camelCase (id, title, completed).
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

// Device line over Tauri invoke, backed by the Rust DeviceClient.
// Invoke argument keys are snake_case to match the Rust
// command parameters exactly.
export class InvokeDeviceClient implements DeviceGateway {
  async listReminders(): Promise<DeviceReminder[]> {
    return invoke<DeviceReminder[]>("device_reminders_list");
  }

  async createAction(
    reminderId: string,
    targetCompleted: boolean,
  ): Promise<DeviceActionRecord> {
    const record = await invoke<ServerDeviceActionRecord>(
      "device_actions_create",
      { reminder_id: reminderId, target_completed: targetCompleted },
    );
    return toRecord(record);
  }

  async setReminder(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord> {
    const record = await invoke<ServerDeviceActionRecord>(
      "device_reminder_set",
      { action_id: actionId, tool_call_id: toolCallId },
    );
    return toRecord(record);
  }

  async markRejected(
    actionId: string,
    toolCallId: string,
  ): Promise<DeviceActionRecord> {
    const record = await invoke<ServerDeviceActionRecord>(
      "device_action_mark_rejected",
      { action_id: actionId, tool_call_id: toolCallId },
    );
    return toRecord(record);
  }

  async refreshAction(actionId: string): Promise<DeviceActionRecord> {
    const record = await invoke<ServerDeviceActionRecord>(
      "device_action_refresh",
      { action_id: actionId },
    );
    return toRecord(record);
  }

  async listActions(): Promise<DeviceActionRecord[]> {
    const records =
      await invoke<ServerDeviceActionRecord[]>("device_actions_list");
    return records.map(toRecord);
  }

  async getAction(actionId: string): Promise<DeviceActionRecord> {
    const record = await invoke<ServerDeviceActionRecord>("device_action_get", {
      action_id: actionId,
    });
    return toRecord(record);
  }
}

import { invoke } from "@tauri-apps/api/core";

// Settings line over Tauri invoke, backed by the Rust
// settings commands. Invoke-only by design: there is no HTTP
// equivalent, so the settings page shows a notice in a plain
// browser preview instead of calling these.
//
// The secret itself never comes back: key status carries only
// configured + a ****last-4 redaction.
export interface SettingsData {
  default_working_dir: string | null;
  working_dir_display: string;
  working_dir_valid: boolean;
  file_root: string;
  working_dir_editable: boolean;
}

export interface KeyStatus {
  configured: boolean;
  redacted: string | null;
}

export class InvokeSettingsClient {
  async get(): Promise<SettingsData> {
    return invoke<SettingsData>("settings_get");
  }

  async pickDirectory(): Promise<string | null> {
    return invoke<string | null>("settings_pick_directory");
  }

  async saveWorkingDir(path: string): Promise<SettingsData> {
    return invoke<SettingsData>("settings_save_working_dir", { path });
  }

  async clearWorkingDir(): Promise<SettingsData> {
    return invoke<SettingsData>("settings_clear_working_dir");
  }

  async keyStatus(): Promise<KeyStatus> {
    return invoke<KeyStatus>("settings_key_status");
  }

  async keySave(secret: string): Promise<KeyStatus> {
    return invoke<KeyStatus>("settings_key_save", { secret });
  }

  async keyRemove(): Promise<KeyStatus> {
    return invoke<KeyStatus>("settings_key_remove");
  }

  async deviceAuthorization(): Promise<string> {
    return invoke<string>("device_authorization");
  }
}

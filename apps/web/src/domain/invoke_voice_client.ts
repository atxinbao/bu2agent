// Invoke voice client: packaged app -> Tauri shell voice_*
// commands (Task 21, record subset, D2). Param names are
// snake_case to match the shell's rename_all; the invoke
// transport returns the Rust structs' snake_case fields, so
// the mapping lives here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  ActiveVoice,
  CatalogFilters,
  CatalogVoice,
  CustomVoice,
  OnboardingReset,
  PipelineProfile,
  PipelineSelection,
  VoiceGateway,
} from "./voice_client";

interface ServerCatalogVoice {
  voice_id: string;
  name: string;
  locale: string;
  available: boolean;
}

interface ServerPipelineProfile {
  profile_id: string;
  name: string;
  description: string;
}

interface ServerCustomVoice {
  saved_voice_id: string;
  name: string;
  created_ms: number;
  updated_ms: number;
  deleted?: boolean;
}

function toCatalogVoice(server: ServerCatalogVoice): CatalogVoice {
  return {
    voiceId: server.voice_id,
    name: server.name,
    locale: server.locale,
    available: server.available,
  };
}

function toPipelineProfile(server: ServerPipelineProfile): PipelineProfile {
  return {
    profileId: server.profile_id,
    name: server.name,
    description: server.description,
  };
}

function toCustomVoice(server: ServerCustomVoice): CustomVoice {
  return {
    savedVoiceId: server.saved_voice_id,
    name: server.name,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted ?? false,
  };
}

export class InvokeVoiceClient implements VoiceGateway {
  async catalog(): Promise<CatalogVoice[]> {
    const voices = await invoke<ServerCatalogVoice[]>("voice_catalog");
    return voices.map(toCatalogVoice);
  }

  catalogFilters(): Promise<CatalogFilters> {
    return invoke("voice_catalog_filters");
  }

  async availableVoices(): Promise<CatalogVoice[]> {
    const voices = await invoke<ServerCatalogVoice[]>("voice_available_voices");
    return voices.map(toCatalogVoice);
  }

  async pipelineProfiles(): Promise<PipelineProfile[]> {
    const profiles = await invoke<ServerPipelineProfile[]>("voice_pipeline_profiles");
    return profiles.map(toPipelineProfile);
  }

  async selectPipelineProfile(profileId: string): Promise<PipelineSelection> {
    const selection = await invoke<{ profile_id: string }>("voice_pipeline_profile_select", {
      profile_id: profileId,
    });
    return { profileId: selection.profile_id };
  }

  async customVoices(): Promise<CustomVoice[]> {
    const voices = await invoke<ServerCustomVoice[]>("voice_custom_voices_list");
    return voices.map(toCustomVoice);
  }

  async renameCustomVoice(savedVoiceId: string, name: string): Promise<CustomVoice> {
    const voice = await invoke<ServerCustomVoice>("voice_custom_voice_rename", {
      saved_voice_id: savedVoiceId,
      name,
    });
    return toCustomVoice(voice);
  }

  async deleteCustomVoice(savedVoiceId: string): Promise<CustomVoice> {
    const voice = await invoke<ServerCustomVoice>("voice_custom_voice_delete", {
      saved_voice_id: savedVoiceId,
    });
    return toCustomVoice(voice);
  }

  async activeVoice(): Promise<ActiveVoice> {
    const active = await invoke<{ voice_id: string | null }>("voice_active_voice_get");
    return { voiceId: active.voice_id };
  }

  async saveActiveVoice(voiceId: string): Promise<ActiveVoice> {
    const active = await invoke<{ voice_id: string | null }>("voice_active_voice_save", {
      voice_id: voiceId,
    });
    return { voiceId: active.voice_id };
  }

  async onboardingReset(): Promise<OnboardingReset> {
    const reset = await invoke<{ voice_id: string | null; profile_id: string | null }>(
      "voice_onboarding_reset",
    );
    return { voiceId: reset.voice_id, profileId: reset.profile_id };
  }
}

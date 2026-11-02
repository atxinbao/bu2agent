// Voice client: browser/dev -> Mock Gateway voice record
// surface (Task 21, record subset, D2).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. These calls
// carry no approval loop. Selections are record-only:
// nothing here plays audio; see the gateway's
// voice_store.rs.

export interface CatalogVoice {
  voiceId: string;
  name: string;
  locale: string;
  available: boolean;
}

export interface CatalogFilters {
  locales: string[];
}

export interface PipelineProfile {
  profileId: string;
  name: string;
  description: string;
}

export interface PipelineSelection {
  profileId: string;
}

export interface CustomVoice {
  savedVoiceId: string;
  name: string;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
}

export interface ActiveVoice {
  voiceId: string | null;
}

export interface OnboardingReset {
  voiceId: string | null;
  profileId: string | null;
}

// Transport-neutral surface used by the Settings view. The
// HTTP client below and the invoke client both implement
// it, so the view never knows which transport it runs on.
export interface VoiceGateway {
  catalog(): Promise<CatalogVoice[]>;
  catalogFilters(): Promise<CatalogFilters>;
  availableVoices(): Promise<CatalogVoice[]>;
  pipelineProfiles(): Promise<PipelineProfile[]>;
  selectPipelineProfile(profileId: string): Promise<PipelineSelection>;
  customVoices(): Promise<CustomVoice[]>;
  renameCustomVoice(savedVoiceId: string, name: string): Promise<CustomVoice>;
  deleteCustomVoice(savedVoiceId: string): Promise<CustomVoice>;
  activeVoice(): Promise<ActiveVoice>;
  saveActiveVoice(voiceId: string): Promise<ActiveVoice>;
  onboardingReset(): Promise<OnboardingReset>;
}

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

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("voice gateway URL must be a localhost Mock");
  }
}

export class VoiceClient implements VoiceGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`voice gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async catalog(): Promise<CatalogVoice[]> {
    const voices = await this.request<ServerCatalogVoice[]>("GET", "/api/voice/catalog");
    return voices.map(toCatalogVoice);
  }

  catalogFilters(): Promise<CatalogFilters> {
    return this.request<CatalogFilters>("GET", "/api/voice/catalog/filters");
  }

  async availableVoices(): Promise<CatalogVoice[]> {
    const voices = await this.request<ServerCatalogVoice[]>(
      "GET",
      "/api/voice/available-voices",
    );
    return voices.map(toCatalogVoice);
  }

  async pipelineProfiles(): Promise<PipelineProfile[]> {
    const profiles = await this.request<ServerPipelineProfile[]>(
      "GET",
      "/api/voice/pipeline-profiles",
    );
    return profiles.map(toPipelineProfile);
  }

  async selectPipelineProfile(profileId: string): Promise<PipelineSelection> {
    const selection = await this.request<{ profile_id: string }>(
      "POST",
      "/api/voice/pipeline-profile",
      { profile_id: profileId },
    );
    return { profileId: selection.profile_id };
  }

  async customVoices(): Promise<CustomVoice[]> {
    const voices = await this.request<ServerCustomVoice[]>("GET", "/api/voice/custom-voices");
    return voices.map(toCustomVoice);
  }

  async renameCustomVoice(savedVoiceId: string, name: string): Promise<CustomVoice> {
    const voice = await this.request<ServerCustomVoice>(
      "PATCH",
      `/api/voice/custom-voices/${savedVoiceId}`,
      { name },
    );
    return toCustomVoice(voice);
  }

  async deleteCustomVoice(savedVoiceId: string): Promise<CustomVoice> {
    const voice = await this.request<ServerCustomVoice>(
      "DELETE",
      `/api/voice/custom-voices/${savedVoiceId}`,
    );
    return toCustomVoice(voice);
  }

  async activeVoice(): Promise<ActiveVoice> {
    const active = await this.request<{ voice_id: string | null }>(
      "GET",
      "/api/voice/active-voice",
    );
    return { voiceId: active.voice_id };
  }

  async saveActiveVoice(voiceId: string): Promise<ActiveVoice> {
    const active = await this.request<{ voice_id: string | null }>(
      "POST",
      "/api/voice/active-voice",
      { voice_id: voiceId },
    );
    return { voiceId: active.voice_id };
  }

  async onboardingReset(): Promise<OnboardingReset> {
    const reset = await this.request<{ voice_id: string | null; profile_id: string | null }>(
      "POST",
      "/api/voice/onboarding/reset",
      {},
    );
    return { voiceId: reset.voice_id, profileId: reset.profile_id };
  }
}

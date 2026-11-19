// Location client: browser/dev -> Mock Gateway location
// surface (MCL-3, read only since S-1).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Read only:
// `recordLocation` throws (see below) — recording
// moved to the packaged app's shell process in S-1
// (Q-S-01/Q-S-08) and the browser never holds the
// startup token. The packaged Mac app records through
// the invoke client; other platforms answer 501 from
// the shell command.
//
// PII shape (Q-MCL-17): SQLite stores the RAW
// coordinates and the UI shows exact values only
// locally in the packaged app, never logged or
// exported. `localityHash` is a coarse grid hash;
// `countryCode` is null when the gateway could not
// determine one.

// One recorded location sample (the gateway
// `DeviceLocationRow` shape, camel-cased).
export interface DeviceLocationSample {
  sampleId: string;
  sourceId: string;
  latitude: number;
  longitude: number;
  accuracyMeters: number;
  localityHash: string;
  countryCode: string | null;
  recordedMs: number;
  createdMs: number;
  originDeviceId: string;
}

// Transport-neutral surface used by the Location view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface LocationGateway {
  recordLocation(): Promise<DeviceLocationSample>;
  samples(limit: number | null, offset: number | null): Promise<DeviceLocationSample[]>;
  sample(sampleId: string): Promise<DeviceLocationSample>;
}

interface ServerDeviceLocationSample {
  sample_id: string;
  source_id: string;
  latitude: number;
  longitude: number;
  accuracy_meters: number;
  locality_hash: string;
  country_code: string | null;
  recorded_ms: number;
  created_ms: number;
  origin_device_id: string;
}

function toDeviceLocationSample(server: ServerDeviceLocationSample): DeviceLocationSample {
  return {
    sampleId: server.sample_id,
    sourceId: server.source_id,
    latitude: server.latitude,
    longitude: server.longitude,
    accuracyMeters: server.accuracy_meters,
    localityHash: server.locality_hash,
    countryCode: server.country_code,
    recordedMs: server.recorded_ms,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("location gateway URL must be a localhost Mock");
  }
}

export class LocationClient implements LocationGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`location gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async recordLocation(): Promise<DeviceLocationSample> {
    // S-1 (Q-S-01/Q-S-08): the browser/dev client can no
    // longer record. The old route (`POST /api/location/record`,
    // gateway-side Core Location fetch) was deleted with
    // the sidecar body, and the new route
    // (`POST /api/location/record-from-shell`) accepts a
    // fix ONLY with the shell startup token — which must
    // never enter the frontend. Recording works only in
    // the packaged app, through the invoke client
    // (`InvokeLocationClient.recordLocation`, same
    // signature, same returned row); the view is
    // unchanged and keeps calling this interface.
    throw new Error(
      "location recording is only available in the packaged app (use the desktop build)",
    );
  }

  async samples(limit: number | null, offset: number | null): Promise<DeviceLocationSample[]> {
    const query = new URLSearchParams();
    if (limit !== null) query.set("limit", String(limit));
    if (offset !== null) query.set("offset", String(offset));
    const suffix = query.toString() === "" ? "" : `?${query.toString()}`;
    const samples = await this.requestJson<ServerDeviceLocationSample[]>("GET", `/api/location/samples${suffix}`);
    return samples.map(toDeviceLocationSample);
  }

  async sample(sampleId: string): Promise<DeviceLocationSample> {
    const sample = await this.requestJson<ServerDeviceLocationSample>(
      "GET",
      `/api/location/samples/${sampleId}`,
    );
    return toDeviceLocationSample(sample);
  }
}

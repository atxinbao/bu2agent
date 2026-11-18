// Invoke location client: packaged app -> Tauri shell
// location_* commands (MCL-3, manual record + read).
// Param names are snake_case to match the shell's
// rename_all; the invoke transport returns the Rust
// structs' snake_case fields, so the mapping lives
// here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  DeviceLocationSample,
  LocationGateway,
} from "./location_client";

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

export class InvokeLocationClient implements LocationGateway {
  async recordLocation(): Promise<DeviceLocationSample> {
    const sample = await invoke<ServerDeviceLocationSample>("location_record");
    return toDeviceLocationSample(sample);
  }

  async samples(limit: number | null, offset: number | null): Promise<DeviceLocationSample[]> {
    const samples = await invoke<ServerDeviceLocationSample[]>("location_list", {
      limit,
      offset,
    });
    return samples.map(toDeviceLocationSample);
  }

  async sample(sampleId: string): Promise<DeviceLocationSample> {
    const sample = await invoke<ServerDeviceLocationSample>("location_get", {
      sample_id: sampleId,
    });
    return toDeviceLocationSample(sample);
  }
}

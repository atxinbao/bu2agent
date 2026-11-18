// Location view: location read surface (MCL-3).
// Samples list -> sample detail. The record button is
// a manual trigger: it records one Core Location
// sample into local SQLite; there is no
// auto-collection and no background track.
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads real system location via Core
// Location; other platforms answer 501. Exact latitude
// / longitude are shown only locally in the packaged
// app (Q-MCL-17).

import { useEffect, useState } from "react";
import type {
  DeviceLocationSample,
  LocationGateway,
} from "../domain/location_client";

interface LocationViewProps {
  location: LocationGateway;
}

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

export default function LocationView({ location }: LocationViewProps) {
  const [rows, setRows] = useState<DeviceLocationSample[]>([]);
  const [selected, setSelected] = useState<DeviceLocationSample | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadSamples() {
    try {
      setRows(await location.samples(50, 0));
    } catch (e) {
      setError(errText(e));
    }
  }

  useEffect(() => {
    void loadSamples();
    // The gateway is stable for the view's lifetime.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function recordLocation() {
    setBusy(true);
    setError(null);
    try {
      const sample = await location.recordLocation();
      setSelected(sample);
      await loadSamples();
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>定位</h2>
      <p className="note">
        手动点“记录当前位置”记一条样本，无自动采集、无后台轨迹。打包的 Mac
        应用读真实系统定位，其他平台返回 501。精确经纬度只在本地显示。
      </p>
      <div className="row">
        <button type="button" onClick={() => void recordLocation()} disabled={busy}>
          记录当前位置
        </button>
        <button type="button" onClick={() => void loadSamples()} disabled={busy}>
          刷新
        </button>
      </div>
      {error !== null && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {rows.length === 0 && <p className="empty">暂无已记录的定位样本。</p>}
      <ul className="entries">
        {rows.map((row) => (
          <li key={row.sampleId}>
            <button type="button" onClick={() => setSelected(row)}>
              {row.sampleId} · 精度 {row.accuracyMeters} 米 · {fmtTime(row.recordedMs)}
            </button>
          </li>
        ))}
      </ul>
      {selected !== null && (
        <div>
          <h3>定位样本 {selected.sampleId}</h3>
          <p className="note">
            样本详情只有行字段；精确经纬度只在本地显示。
          </p>
          <pre className="result">
            {[
              `sample_id: ${selected.sampleId}`,
              `latitude: ${selected.latitude}`,
              `longitude: ${selected.longitude}`,
              `accuracy_meters: ${selected.accuracyMeters}`,
              `locality_hash: ${selected.localityHash}`,
              `country_code: ${selected.countryCode ?? "未知"}`,
              `recorded: ${fmtTime(selected.recordedMs)}`,
              `created: ${fmtTime(selected.createdMs)}`,
            ].join("\n")}
          </pre>
        </div>
      )}
    </div>
  );
}

import { requestJson, type RecordedResponse } from "./http";
import type { HealthProbe, QueueSnapshot, TrustCase } from "../types";

export function toggleSchema(enabled: boolean): Promise<RecordedResponse<{ schema_version: number; confidence_level: boolean }>> {
  return requestJson("/demo/admin/schema-version", {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}

export function toggleOutage(enabled: boolean): Promise<RecordedResponse<{ outage: boolean }>> {
  return requestJson("/demo/admin/outage", {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}

export function fetchServiceHealth(): Promise<RecordedResponse<{ checked_at: string; services: HealthProbe[] }>> {
  return requestJson("/demo/observability/health");
}

export function fetchQueues(): Promise<RecordedResponse<{ checked_at: string; queues: QueueSnapshot[] }>> {
  return requestJson("/demo/observability/queues");
}

export function runCrossCredentials(): Promise<RecordedResponse<{ passed: boolean; cases: TrustCase[] }>> {
  return requestJson("/demo/trust/cross-credentials", { method: "POST" });
}

export function listArtifacts(): Promise<RecordedResponse<{ artifacts: string[] }>> {
  return requestJson("/demo/artifacts");
}

export function getArtifact(name: string): Promise<RecordedResponse<{ name: string; content: unknown }>> {
  return requestJson(`/demo/artifacts/${encodeURIComponent(name)}`);
}

export function fetchOwnership(): Promise<RecordedResponse<Record<string, unknown>>> {
  return requestJson("/demo/ownership");
}

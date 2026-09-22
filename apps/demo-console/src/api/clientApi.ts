import { requestJson, type RecordedResponse } from "./http";
import type { HazardListResponse, RawHazard, SourceStatus, SummaryHazard, TokenPair } from "../types";

export function issueToken(clientId: string, clientSecret: string): Promise<RecordedResponse<TokenPair>> {
  return requestJson<TokenPair>("/api/auth/token", {
    method: "POST",
    body: JSON.stringify({ client_id: clientId, client_secret: clientSecret }),
  });
}

export function refreshToken(refreshTokenValue: string): Promise<RecordedResponse<TokenPair>> {
  return requestJson<TokenPair>("/api/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token: refreshTokenValue }),
  });
}

export type HazardQuery = {
  source?: string;
  hazardType?: string;
  raw?: boolean;
  pageSize?: number;
  pageToken?: string;
};

export function listHazards(accessToken: string, query: HazardQuery = {}): Promise<RecordedResponse<HazardListResponse>> {
  const params = new URLSearchParams();
  if (query.source) params.set("source", query.source);
  if (query.hazardType) params.set("hazard_type", query.hazardType);
  if (query.raw) params.set("raw", "true");
  if (query.pageSize) params.set("page_size", String(query.pageSize));
  if (query.pageToken) params.set("page_token", query.pageToken);
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return requestJson<HazardListResponse>(`/api/hazards${suffix}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
}

export function getHazard(accessToken: string, hazardId: string, raw = false): Promise<RecordedResponse<SummaryHazard | RawHazard>> {
  const suffix = raw ? "?raw=true" : "";
  return requestJson<SummaryHazard | RawHazard>(`/api/hazards/${encodeURIComponent(hazardId)}${suffix}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
}

export function sourceStatus(accessToken: string, source: "BMKG" | "PVMBG"): Promise<RecordedResponse<SourceStatus>> {
  return requestJson<SourceStatus>(`/api/sources/status?source=${source}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
}

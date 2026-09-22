export type ClientId = "media-client" | "field-team-client" | "bnpb-ops-client";

export type TokenPair = {
  access_token: string;
  refresh_token: string;
  expires_at: string;
  client_id: string;
  scopes: string[];
};

export type Session = {
  clientId: ClientId;
  accessToken: string;
  refreshToken: string;
  expiresAt: string;
  scopes: string[];
};

export type SummaryHazard = {
  hazard_id: string;
  source: string;
  hazard_type: string;
  severity: string;
  area_name: string;
  occurred_at: string;
  ingested_at: string;
};

export type RawHazard = SummaryHazard & {
  source_ref_id?: string;
  latitude?: number;
  longitude?: number;
  attributes?: Record<string, unknown>;
};

export type HazardListResponse = {
  hazards: Array<SummaryHazard | RawHazard>;
  next_page_token?: string;
};

export type SourceStatus = {
  source?: number | string;
  status: string;
  last_success_at?: string;
  stale_since?: string;
  last_error?: string;
};

export type ApiError = {
  error?: string;
};

export type EvidenceStatus = "PASS" | "FAIL" | "WAITING";

export type EvidenceEntry = {
  id: string;
  problem: "P1" | "P2" | "P3" | "P4" | "P5" | "GENERAL";
  criterion: string;
  timestamp: string;
  method: string;
  url: string;
  correlationId: string;
  durationMs: number;
  expected: string;
  actual: string;
  status: EvidenceStatus;
};

export type HealthProbe = {
  name: string;
  status: string;
  detail?: string;
  optional?: boolean;
};

export type QueueSnapshot = {
  name: string;
  messages: number;
  consumers: number;
  ready: number;
};

export type TrustCase = {
  name: string;
  target: string;
  credential_used: string;
  status_code: number;
  passed: boolean;
};

export type AssertionResult = {
  ok: boolean;
  expected: string;
  actual: string;
};

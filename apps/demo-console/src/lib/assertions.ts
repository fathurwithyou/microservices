import type { AssertionResult, RawHazard, SummaryHazard } from "../types";

const SUMMARY_FIELDS = ["hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"] as const;
const RAW_FIELDS = ["source_ref_id", "latitude", "longitude", "attributes"] as const;

export function requiredCanonicalFields(hazard: SummaryHazard | RawHazard): AssertionResult {
  const missing = SUMMARY_FIELDS.filter((field) => hazard[field] === undefined || hazard[field] === "");
  return {
    ok: missing.length === 0,
    expected: `canonical fields present: ${SUMMARY_FIELDS.join(", ")}`,
    actual: missing.length === 0 ? "all required fields present" : `missing ${missing.join(", ")}`,
  };
}

export function isSummaryOnly(hazard: Record<string, unknown>): AssertionResult {
  const leaked = RAW_FIELDS.filter((field) => hazard[field] !== undefined);
  return {
    ok: leaked.length === 0,
    expected: "summary payload without latitude, longitude, source_ref_id, or attributes",
    actual: leaked.length === 0 ? "summary only" : `leaked ${leaked.join(", ")}`,
  };
}

export function hasConfidenceLevel(hazard: RawHazard): boolean {
  const attributes = hazard.attributes ?? {};
  return attributes.confidence_level !== undefined && attributes.confidence_level !== null;
}

export function splitByConfidence(hazards: RawHazard[]): { without: RawHazard[]; withField: RawHazard[] } {
  return {
    without: hazards.filter((hazard) => !hasConfidenceLevel(hazard)),
    withField: hazards.filter((hazard) => hasConfidenceLevel(hazard)),
  };
}

export function httpExpected(status: number, allowed: number[]): AssertionResult {
  return {
    ok: allowed.includes(status),
    expected: `HTTP ${allowed.join(" or ")}`,
    actual: `HTTP ${status}`,
  };
}

export function statusIsStale(status: string, staleSince?: string): AssertionResult {
  return {
    ok: status === "stale" && Boolean(staleSince),
    expected: "source status stale with stale_since",
    actual: `status=${status} stale_since=${staleSince || "(empty)"}`,
  };
}

export function evidenceFromAssertion(
  assertion: AssertionResult,
): "PASS" | "FAIL" {
  return assertion.ok ? "PASS" : "FAIL";
}

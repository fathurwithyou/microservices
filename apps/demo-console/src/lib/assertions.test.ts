import { hasConfidenceLevel, httpExpected, isSummaryOnly, requiredCanonicalFields, splitByConfidence } from "./assertions";
import type { RawHazard } from "../types";

const base: RawHazard = {
  hazard_id: "h-1",
  source: "SOURCE_PVMBG",
  hazard_type: "HAZARD_TYPE_VOLCANIC",
  severity: "SEVERITY_SIAGA",
  area_name: "Merapi",
  occurred_at: "2026-09-22T01:00:00Z",
  ingested_at: "2026-09-22T01:00:02Z",
};

describe("canonical field assertions", () => {
  it("accepts a complete hazard", () => {
    expect(requiredCanonicalFields(base).ok).toBe(true);
  });

  it("rejects missing area", () => {
    expect(requiredCanonicalFields({ ...base, area_name: "" }).ok).toBe(false);
  });
});

describe("summary vs raw", () => {
  it("fails when summary contains coordinates", () => {
    expect(isSummaryOnly({ ...base, latitude: -7.5 }).ok).toBe(false);
  });

  it("passes summary-only payloads", () => {
    expect(isSummaryOnly({ ...base }).ok).toBe(true);
  });
});

describe("schema evolution helpers", () => {
  it("detects confidence_level in attributes", () => {
    expect(hasConfidenceLevel({ ...base, attributes: { confidence_level: 0.82 } })).toBe(true);
  });

  it("splits old and new records", () => {
    const split = splitByConfidence([
      { ...base, hazard_id: "old" },
      { ...base, hazard_id: "new", attributes: { confidence_level: 0.9 } },
    ]);
    expect(split.without).toHaveLength(1);
    expect(split.withField).toHaveLength(1);
  });
});

describe("httpExpected", () => {
  it("treats 403 as success when allowed", () => {
    expect(httpExpected(403, [401, 403]).ok).toBe(true);
  });
});

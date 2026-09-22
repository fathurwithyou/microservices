import { canRequestRaw, formatCountdown, isExpired, severityTone, sourceName } from "./clients";

describe("client helpers", () => {
  it("denies raw scope for media", () => {
    expect(canRequestRaw(["hazard:read:summary"])).toBe(false);
    expect(canRequestRaw(["hazard:read:summary", "hazard:read:raw"])).toBe(true);
  });

  it("formats TTL countdown", () => {
    expect(formatCountdown(new Date(Date.now() + 5500).toISOString(), Date.now())).toBe("6s");
    expect(formatCountdown(new Date(Date.now() - 1000).toISOString(), Date.now())).toBe("expired");
  });

  it("detects expiry", () => {
    expect(isExpired(new Date(Date.now() - 1).toISOString())).toBe(true);
  });

  it("maps source enums", () => {
    expect(sourceName(1)).toBe("BMKG");
    expect(sourceName("SOURCE_PVMBG")).toBe("PVMBG");
  });

  it("maps severity tones", () => {
    expect(severityTone("SEVERITY_AWAS")).toBe("critical");
    expect(severityTone("SEVERITY_NORMAL")).toBe("normal");
  });
});

import type { ClientId } from "../types";

export const CLIENTS: { id: ClientId; label: string; rawAccess: boolean; description: string }[] = [
  { id: "media-client", label: "Media / Pers", rawAccess: false, description: "Ringkasan saja. Permintaan field mentah wajib 403." },
  { id: "field-team-client", label: "Tim Lapangan", rawAccess: true, description: "Seluruh field termasuk mentah. Access token TTL pendek + refresh." },
  { id: "bnpb-ops-client", label: "BNPB Ops", rawAccess: true, description: "Baseline hak akses tertinggi, identitas terpisah." },
];

export function clientLabel(id: ClientId): string {
  return CLIENTS.find((client) => client.id === id)?.label ?? id;
}

export function canRequestRaw(scopes: string[]): boolean {
  return scopes.includes("hazard:read:raw");
}

export function formatCountdown(expiresAt: string, now = Date.now()): string {
  const remaining = Date.parse(expiresAt) - now;
  if (Number.isNaN(remaining)) return "unknown";
  if (remaining <= 0) return "expired";
  const seconds = Math.ceil(remaining / 1000);
  return `${seconds}s`;
}

export function isExpired(expiresAt: string, now = Date.now()): boolean {
  const timestamp = Date.parse(expiresAt);
  return Number.isNaN(timestamp) || timestamp <= now;
}

export function sourceName(value: number | string | undefined): string {
  if (value === 1 || value === "SOURCE_BMKG" || value === "BMKG") return "BMKG";
  if (value === 2 || value === "SOURCE_PVMBG" || value === "PVMBG") return "PVMBG";
  return String(value ?? "unknown");
}

export function severityTone(severity: string): "normal" | "watch" | "alert" | "critical" {
  const value = severity.toUpperCase();
  if (value.includes("AWAS")) return "critical";
  if (value.includes("SIAGA")) return "alert";
  if (value.includes("WASPADA")) return "watch";
  return "normal";
}

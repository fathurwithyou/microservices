import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import type { EvidenceEntry, EvidenceStatus } from "../types";

type EvidenceContextValue = {
  entries: EvidenceEntry[];
  record: (entry: Omit<EvidenceEntry, "id" | "timestamp"> & { id?: string; timestamp?: string }) => EvidenceEntry;
  clear: () => void;
  download: () => void;
};

const EvidenceContext = createContext<EvidenceContextValue | undefined>(undefined);

export function EvidenceProvider({ children }: { children: ReactNode }) {
  const [entries, setEntries] = useState<EvidenceEntry[]>([]);

  const record = useCallback((entry: Omit<EvidenceEntry, "id" | "timestamp"> & { id?: string; timestamp?: string }) => {
    const complete: EvidenceEntry = {
      id: entry.id ?? crypto.randomUUID(),
      timestamp: entry.timestamp ?? new Date().toISOString(),
      ...entry,
    };
    setEntries((current) => [complete, ...current]);
    return complete;
  }, []);

  const clear = useCallback(() => setEntries([]), []);

  const download = useCallback(() => {
    const blob = new Blob([JSON.stringify({ generated_at: new Date().toISOString(), entries }, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `bnpb-m1-evidence-${Date.now()}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }, [entries]);

  const value = useMemo(() => ({ entries, record, clear, download }), [entries, record, clear, download]);
  return <EvidenceContext.Provider value={value}>{children}</EvidenceContext.Provider>;
}

export function useEvidence(): EvidenceContextValue {
  const value = useContext(EvidenceContext);
  if (!value) {
    throw new Error("EvidenceProvider required");
  }
  return value;
}

export function statusFromOk(ok: boolean): EvidenceStatus {
  return ok ? "PASS" : "FAIL";
}

import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { issueToken, refreshToken } from "../api/clientApi";
import { isExpired } from "../lib/clients";
import type { ClientId, Session } from "../types";

const STORAGE_KEY = "bnpb-demo-sessions";

type SessionsState = Partial<Record<ClientId, Session>>;

type SessionsContextValue = {
  sessions: SessionsState;
  activeClientId: ClientId;
  setActiveClientId: (id: ClientId) => void;
  login: (clientId: ClientId, secret: string) => Promise<{ ok: boolean; message: string }>;
  refresh: (clientId: ClientId) => Promise<{ ok: boolean; message: string; previousRefreshRejected?: boolean; accessToken?: string }>;
  logout: (clientId: ClientId) => void;
  session: Session | undefined;
};

const SessionsContext = createContext<SessionsContextValue | undefined>(undefined);

function readStored(): SessionsState {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as SessionsState) : {};
  } catch {
    return {};
  }
}

export function SessionsProvider({ children }: { children: ReactNode }) {
  const [sessions, setSessions] = useState<SessionsState>(readStored);
  const [activeClientId, setActiveClientId] = useState<ClientId>("field-team-client");

  const persist = useCallback((next: SessionsState) => {
    setSessions(next);
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }, []);

  const login = useCallback(async (clientId: ClientId, secret: string) => {
    const response = await issueToken(clientId, secret);
    if (!response.ok || !response.data?.access_token) {
      return { ok: false, message: response.error || `HTTP ${response.status}` };
    }
    const nextSession: Session = {
      clientId,
      accessToken: response.data.access_token,
      refreshToken: response.data.refresh_token,
      expiresAt: response.data.expires_at,
      scopes: response.data.scopes ?? [],
    };
    persist({ ...sessions, [clientId]: nextSession });
    setActiveClientId(clientId);
    return { ok: true, message: `token issued, TTL until ${response.data.expires_at}` };
  }, [persist, sessions]);

  const refresh = useCallback(async (clientId: ClientId) => {
    const current = sessions[clientId];
    if (!current) {
      return { ok: false, message: "no session" };
    }
    const oldRefresh = current.refreshToken;
    const response = await refreshToken(oldRefresh);
    if (!response.ok || !response.data?.access_token) {
      return { ok: false, message: response.error || `HTTP ${response.status}` };
    }
    persist({
      ...sessions,
      [clientId]: {
        clientId,
        accessToken: response.data.access_token,
        refreshToken: response.data.refresh_token,
        expiresAt: response.data.expires_at,
        scopes: response.data.scopes ?? current.scopes,
      },
    });
    const replay = await refreshToken(oldRefresh);
    return {
      ok: true,
      message: "refresh rotated",
      previousRefreshRejected: replay.status === 401,
      accessToken: response.data.access_token,
    };
  }, [persist, sessions]);

  const logout = useCallback((clientId: ClientId) => {
    const next = { ...sessions };
    delete next[clientId];
    persist(next);
  }, [persist, sessions]);

  const value = useMemo<SessionsContextValue>(
    () => ({
      sessions,
      activeClientId,
      setActiveClientId,
      login,
      refresh,
      logout,
      session: sessions[activeClientId] && !isExpired(sessions[activeClientId]!.expiresAt)
        ? sessions[activeClientId]
        : sessions[activeClientId],
    }),
    [sessions, activeClientId, login, refresh, logout],
  );

  return <SessionsContext.Provider value={value}>{children}</SessionsContext.Provider>;
}

export function useSessions(): SessionsContextValue {
  const value = useContext(SessionsContext);
  if (!value) {
    throw new Error("SessionsProvider required");
  }
  return value;
}

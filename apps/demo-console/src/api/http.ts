export function newCorrelationId(): string {
  return crypto.randomUUID();
}

export type RecordedResponse<T> = {
  ok: boolean;
  status: number;
  data: T | null;
  error: string;
  correlationId: string;
  durationMs: number;
  url: string;
  method: string;
};

export async function requestJson<T>(
  url: string,
  init: RequestInit & { correlationId?: string } = {},
): Promise<RecordedResponse<T>> {
  const correlationId = init.correlationId ?? newCorrelationId();
  const method = init.method ?? "GET";
  const started = performance.now();
  const headers = new Headers(init.headers);
  headers.set("X-Correlation-ID", correlationId);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  try {
    const response = await fetch(url, { ...init, headers });
    const durationMs = Math.round(performance.now() - started);
    const text = await response.text();
    let parsed: unknown = null;
    if (text) {
      try {
        parsed = JSON.parse(text) as unknown;
      } catch {
        parsed = { error: text };
      }
    }
    const error =
      !response.ok && parsed && typeof parsed === "object" && "error" in parsed
        ? String((parsed as { error?: unknown }).error ?? response.statusText)
        : response.ok
          ? ""
          : response.statusText;
    return {
      ok: response.ok,
      status: response.status,
      data: response.ok ? (parsed as T) : (parsed as T | null),
      error,
      correlationId: response.headers.get("X-Correlation-ID") ?? correlationId,
      durationMs,
      url,
      method,
    };
  } catch (caught) {
    return {
      ok: false,
      status: 0,
      data: null,
      error: caught instanceof Error ? caught.message : "network error",
      correlationId,
      durationMs: Math.round(performance.now() - started),
      url,
      method,
    };
  }
}

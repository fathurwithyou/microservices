import { httpExpected } from "../lib/assertions";
import { listHazards } from "./clientApi";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

const server = setupServer(
  http.get("/api/hazards", ({ request }) => {
    const url = new URL(request.url);
    if (url.searchParams.get("raw") === "true") {
      return HttpResponse.json({ error: "raw scope required" }, { status: 403 });
    }
    return HttpResponse.json({
      hazards: [
        {
          hazard_id: "h-1",
          source: "SOURCE_BMKG",
          hazard_type: "HAZARD_TYPE_SEISMIC",
          severity: "SEVERITY_WASPADA",
          area_name: "Banten",
          occurred_at: "2026-09-22T01:00:00Z",
          ingested_at: "2026-09-22T01:00:01Z",
        },
      ],
    });
  }),
);

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("client API contract", () => {
  it("records 403 when media requests raw fields", async () => {
    const response = await listHazards("media-token", { raw: true });
    expect(response.status).toBe(403);
    expect(httpExpected(response.status, [403]).ok).toBe(true);
    expect(response.correlationId).toBeTruthy();
  });
});

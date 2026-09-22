import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import SessionBar from "../components/SessionBar";
import { EvidenceProvider } from "../state/evidence";
import { SessionsProvider } from "../state/sessions";

const server = setupServer(
  http.post("/api/auth/token", async ({ request }) => {
    const body = (await request.json()) as { client_id: string };
    if (body.client_id === "media-client") {
      return HttpResponse.json({
        access_token: "media-access",
        refresh_token: "media-refresh",
        expires_at: new Date(Date.now() + 60_000).toISOString(),
        client_id: "media-client",
        scopes: ["hazard:read:summary"],
      });
    }
    return HttpResponse.json({
      access_token: "field-access",
      refresh_token: "field-refresh",
      expires_at: new Date(Date.now() + 60_000).toISOString(),
      client_id: "field-team-client",
      scopes: ["hazard:read:summary", "hazard:read:raw"],
    });
  }),
);

beforeAll(() => server.listen());
afterEach(() => {
  server.resetHandlers();
  sessionStorage.clear();
});
afterAll(() => server.close());

function renderBar() {
  return render(
    <MemoryRouter>
      <SessionsProvider>
        <EvidenceProvider>
          <SessionBar />
        </EvidenceProvider>
      </SessionsProvider>
    </MemoryRouter>,
  );
}

describe("session authorization rendering", () => {
  it("shows summary-only after media login and raw scope after field login", async () => {
    const user = userEvent.setup();
    renderBar();
    const secrets = screen.getAllByPlaceholderText("client_secret dari .env");
    await user.type(secrets[0], "media-secret");
    await user.click(screen.getAllByRole("button", { name: "Issue token" })[0]);
    expect(await screen.findByText("summary only")).toBeInTheDocument();

    await user.type(screen.getAllByPlaceholderText("client_secret dari .env")[0], "field-secret");
    await user.click(screen.getAllByRole("button", { name: "Issue token" })[0]);
    expect(await screen.findByText("raw scope")).toBeInTheDocument();
  });
});

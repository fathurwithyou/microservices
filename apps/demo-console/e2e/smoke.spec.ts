import { test, expect } from "@playwright/test";

const mediaSecret = process.env.MEDIA_CLIENT_SECRET;
const fieldSecret = process.env.FIELD_TEAM_CLIENT_SECRET;

test.describe("demo console smoke", () => {
  test("renders overview and session cards", async ({ page }) => {
    const response = await page.goto("/");
    test.skip(!response || response.status() >= 400, "demo console is not running");
    await expect(page.getByRole("heading", { name: "BNPB Demo Console" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Media / Pers" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Tim Lapangan" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "BNPB Ops" })).toBeVisible();
  });

  test("media raw request is rejected with 403", async ({ page }) => {
    test.skip(!mediaSecret || !fieldSecret, "client secrets not provided");
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    await page.goto("/hazards");
    const secretInputs = page.getByPlaceholder("client_secret dari .env");
    await secretInputs.nth(0).fill(mediaSecret!);
    await page.getByRole("button", { name: "Issue token" }).nth(0).click();
    await expect(page.getByText("summary only")).toBeVisible();
    await page.getByLabel(/raw=true/).check();
    const rawResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/api/hazards") && url.searchParams.get("raw") === "true";
    });
    await page.getByRole("button", { name: "Fetch" }).click();
    expect((await rawResponse).status()).toBe(403);
    await expect(page.getByText(/Server menolak raw scope \(403\)/)).toBeVisible({ timeout: 15_000 });
    expect(pageErrors, pageErrors.join("\n")).toEqual([]);
  });
});

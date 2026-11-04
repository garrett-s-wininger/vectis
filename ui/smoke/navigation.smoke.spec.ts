import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [1280, 820, 520, 390]) {
    test(`help and account menus ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 900 });
      await page.route("**/ui/api/context", (route) =>
        route.fulfill({
          json: {
            auth_enabled: true,
            principal: { kind: "local_user", username: "admin" },
            docs_url: "https://docs.example.test/vectis/"
          }
        })
      );

      await page.goto("/runs");

      const help = page.getByLabel("Help", { exact: true });
      await help.click();
      const docs = page.getByRole("link", { name: "Documentation (opens in a new tab)" });
      await expect(docs).toHaveAttribute("href", "https://docs.example.test/vectis/docs/getting-started");
      await expect(page.getByRole("link", { name: "API reference (opens in a new tab)" })).toHaveAttribute(
        "href",
        "https://docs.example.test/vectis/api"
      );

      await expect(docs).toHaveAttribute("target", "_blank");

      for (const name of ["Documentation", "API reference", "Source code", "Report an issue"]) {
        const link = page.getByRole("link", { name: `${name} (opens in a new tab)` });
        await expect(link).toBeVisible();

        const bounds = (await link.boundingBox())!;
        expect(bounds.x).toBeGreaterThanOrEqual(0);
        expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);
      }

      await page.screenshot({ path: testInfo.outputPath("help.png") });
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.getByLabel("Account menu for admin", { exact: true }).click();
      await expect(docs).not.toBeVisible();
      await expect(page.getByRole("link", { name: "Profile", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Sign out", exact: true })).toBeVisible();

      await page.screenshot({ path: testInfo.outputPath("account.png") });
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.keyboard.press("Escape");
      await expect(page.getByRole("link", { name: "Profile", exact: true })).not.toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }
}

test("copies full run and job IDs using browser clipboard permissions", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("/runs/run-1240");
  await page.getByRole("button", { name: "Copy Run ID", exact: true }).click();
  await expect(page.getByText("Run ID copied.", { exact: true })).toBeAttached();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe("run-1240");

  await page.goto("/jobs/job-api-test-suite");
  const job = page.getByRole("button", { name: "Copy Job ID", exact: true });
  await expect(job).toBeVisible();

  await job.click();
  await expect(page.getByText("Job ID copied.", { exact: true })).toBeAttached();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe("job-api-test-suite");
});

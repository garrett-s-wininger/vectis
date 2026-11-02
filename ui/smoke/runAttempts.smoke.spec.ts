import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

test.use({ screenshot: "only-on-failure" });

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [1280, 390]) {
    test(`shared attempt context filters the graph logs and artifacts ${colorScheme} ${width}px`, async ({
      page
    }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--shared-attempt-context&viewMode=story");
      const graph = page.getByRole("region", { name: "Graph" });
      const controls = graph.getByRole("group", { name: "Run investigation controls" });
      await expect(page.getByRole("region", { name: "Run summary" }).getByLabel(/^Attempt:/)).toHaveCount(0);

      const logs = page.getByRole("region", { name: "Task Logs", exact: true });
      await expect(page.getByRole("link", { name: "Download report-attempt-2", exact: true })).toBeVisible({
        timeout: 20000
      });

      await expect(page.getByLabel(/^Attempt:/)).toHaveCount(1);

      const header = graph.getByRole("heading", { name: "Graph", exact: true }).locator("..").locator("..");
      await expect(header.getByLabel(/^Attempt:/)).toBeVisible();

      if (width >= 640) {
        const headingBounds = (await header.getByRole("heading").boundingBox())!;
        const actionBounds = (await controls.boundingBox())!;
        expect(actionBounds.x).toBeGreaterThan(headingBounds.x + headingBounds.width);
        expect(actionBounds.y).toBeLessThan(headingBounds.y + headingBounds.height);
      }

      await page.getByRole("button", { name: "Report: Succeeded", exact: true }).click();
      await controls.getByLabel(/^Attempt:/).click();
      await page.getByRole("listbox").getByRole("option", { name: "Attempt 1", exact: true }).click();
      await expect(page.getByRole("button", { name: "Report: Failed", exact: true })).toHaveAttribute(
        "aria-pressed",
        "true"
      );

      await expect(logs.getByText("Failed", { exact: true })).toBeVisible();
      await expect(page.getByRole("link", { name: "Download report-attempt-1", exact: true })).toBeVisible();
      await expect(page.getByRole("link", { name: "Download report-attempt-2", exact: true })).toHaveCount(0);

      await page.getByRole("button", { name: "Archive: Succeeded", exact: true }).click();
      await expect(controls.getByLabel(/^Attempt:/)).toHaveText("Attempt 1");
      await expect(page.getByRole("link", { name: "Download archive-attempt-1", exact: true })).toBeVisible();
      await expect(controls.getByRole("button", { name: "Clear task selection" })).toBeVisible();

      await graph.screenshot({ path: testInfo.outputPath("investigation-controls.png") });
      await expect(page.getByRole("link", { name: "Download report-attempt-1", exact: true })).toHaveCount(0);

      await controls.getByLabel(/^Attempt:/).click();
      await page.getByRole("listbox").getByRole("option", { name: "Attempt 2", exact: true }).click();
      await expect(page.getByRole("button", { name: "Archive: Planned", exact: true })).toHaveAttribute(
        "aria-pressed",
        "true"
      );

      await expect(logs.getByText("This task has no execution in attempt 2.", { exact: true })).toBeVisible();
      await expect(page.getByText("No artifacts for this task in attempt 2.", { exact: true })).toBeVisible();

      await controls.getByLabel(/^Attempt:/).click();
      await expect(page.getByRole("listbox").getByRole("option", { name: "All attempts", exact: true })).toHaveCount(0);

      await page.getByRole("listbox").getByRole("option", { name: "Latest (attempt 2)", exact: true }).click();
      await controls.getByRole("button", { name: "Clear task selection" }).click();
      await expect(page.getByRole("heading", { name: "Run Logs", exact: true })).toBeVisible();
      await expect(page.getByRole("link", { name: "Download legacy-output", exact: true })).toHaveCount(0);
      await expect(page.getByRole("link", { name: "Download report-attempt-2", exact: true })).toBeVisible();

      await graph.scrollIntoViewIfNeeded();
      await page.screenshot({ path: testInfo.outputPath("shared-attempt-context.png"), fullPage: true });
      const a11y = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
      expect(a11y.violations).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`retained task attempts show their own execution and outcome ${colorScheme}`, async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.emulateMedia({ colorScheme });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--retried-run&viewMode=story");
    const logs = page.getByRole("region", { name: "Task Logs", exact: true });
    await page.getByRole("group", { name: "Execution graph", exact: true }).getByRole("button").click();
    await expect(logs.getByText("exec-latest", { exact: true })).toBeVisible({ timeout: 15_000 });

    await page
      .getByRole("region", { name: "Graph" })
      .getByLabel(/^Attempt: /)
      .click();
    await page.getByRole("listbox").getByRole("option", { name: "Attempt 1", exact: true }).click();
    await expect(logs.getByText("exec-old", { exact: true })).toBeVisible();
    await expect(logs.getByText("Failed", { exact: true })).toBeVisible();
    await expect(logs.getByText("Succeeded", { exact: true })).toHaveCount(0);

    await page
      .getByRole("region", { name: "Graph" })
      .getByLabel(/^Attempt: /)
      .click();
    await page.getByRole("listbox").getByRole("option", { name: "Attempt 2", exact: true }).click();
    await expect(logs.getByText("exec-latest", { exact: true })).toBeVisible();
    await expect(logs.getByText("Succeeded", { exact: true })).toBeVisible();

    await page
      .getByRole("region", { name: "Graph" })
      .getByLabel(/^Attempt: /)
      .click();
    await expect(page.getByRole("listbox").getByRole("option", { name: "All attempts", exact: true })).toHaveCount(0);
    await page.getByRole("listbox").getByRole("option", { name: "Latest (attempt 2)", exact: true }).click();
    await expect(logs.getByText("exec-latest", { exact: true })).toBeVisible();
    expect(errors).toEqual([]);
  });
}

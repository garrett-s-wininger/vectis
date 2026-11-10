import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { visibleTextContrast } from "./contrast";
for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280]) {
    test(`captured run definition ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--run-only-definition&viewMode=story");
      const heading = page.getByRole("heading", { name: "Job Definition", exact: true });
      await expect(heading).toBeVisible({ timeout: 20000 });

      const summary = page.getByRole("region", { name: "Run summary", exact: true });
      await expect(summary.getByText("Hotfix", { exact: true })).toBeVisible();
      await expect(summary.getByText("mira", { exact: true })).toHaveCount(1);

      const job = summary.getByRole("link", { name: "View job definition for platform.build", exact: true });
      await expect(job).toHaveAttribute(
        "href",
        "/jobs/platform.build?repository=monorepo&namespace=%2Fteam-a#job-definition-title"
      );

      expect((await job.boundingBox())!.height).toBeGreaterThanOrEqual(40);
      await expect(summary.getByRole("region", { name: "Reason for changes", exact: true })).toContainText(
        "repaired checkout step"
      );

      await summary.screenshot({ path: testInfo.outputPath("run-summary.png") });
      await heading.click();
      const region = page.getByRole("region", { name: "Job Definition", exact: true });
      await expect(
        page.getByText("Run the release validation with a repaired checkout step.", { exact: true })
      ).toHaveCount(1);

      await expect(region.getByText("mira", { exact: true })).toHaveCount(0);
      await expect(region.locator("pre")).toContainText("echo captured definition");
      await expect(region).toContainText("monorepo · .vectis/platform.build.json");
      await expect(region.getByRole("button", { name: "Copy run definition", exact: true })).toBeVisible();
      expect(await page.locator("main").evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await region.screenshot({ path: testInfo.outputPath("run-definition.png") });
    });
  }
}

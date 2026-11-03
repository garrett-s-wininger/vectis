import { expect, test, type Page } from "@playwright/test";

async function expectAttachedCopyField(page: Page, label: string) {
  const button = page.getByRole("button", { name: `Copy ${label}`, exact: true });
  await expect(button).toBeVisible();

  const geometry = await button.evaluate((element) => {
    const field = element.parentElement!;
    const value = field.firstElementChild as HTMLElement;
    const term = field.closest("dd")!.parentElement!.querySelector("dt")!;
    const box = field.getBoundingClientRect();
    const copy = element.getBoundingClientRect();

    return {
      fieldRight: box.right,
      copyRight: copy.right,
      copyWidth: copy.width,
      fieldTop: box.top,
      labelBottom: term.getBoundingClientRect().bottom,
      overflows: value.scrollWidth > value.clientWidth
    };
  });

  expect(geometry.labelBottom).toBeLessThan(geometry.fieldTop);
  expect(geometry.copyRight).toBeCloseTo(geometry.fieldRight - 1, 0);
  expect(geometry.copyWidth).toBeGreaterThanOrEqual(40);
  expect(geometry.overflows).toBe(false);

  await button.focus();
  expect(await button.locator("..").evaluate((element) => getComputedStyle(element).outlineStyle)).not.toBe("none");
}

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [1280, 820, 390]) {
    test(`detail metadata and actions ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1100 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--replay-metadata&viewMode=story");
      await expect(page.getByRole("heading", { name: "Run once (#2)", exact: true })).toBeVisible({ timeout: 20000 });
      await expectAttachedCopyField(page, "Run ID");
      await expectAttachedCopyField(page, "Job ID");

      const reference = page.getByRole("link", { name: /^View original run/ });
      expect((await reference.boundingBox())!.height).toBeGreaterThanOrEqual(42);

      if (width >= 820) {
        const fields = await page
          .getByRole("region", { name: "Run summary" })
          .locator("dd")
          .filter({ has: page.locator("button, a") })
          .evaluateAll((elements) => elements.map((element) => element.getBoundingClientRect().top));

        expect(fields.length).toBe(3);
        expect(Math.max(...fields) - Math.min(...fields)).toBeLessThanOrEqual(1);
      }

      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      await page.mouse.click(8, 8);
      await page.screenshot({ path: testInfo.outputPath("run-details.png") });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-jobs--crud&viewMode=story");
      await page.getByText("api-test-suite", { exact: true }).click();
      await expect(page.getByRole("heading", { name: "api-test-suite", exact: true })).toBeVisible();
      await expectAttachedCopyField(page, "Job ID");

      const allRuns = await page
        .getByRole("button", { name: "View all runs for api-test-suite", exact: true })
        .boundingBox();

      const latestRun = await page
        .getByRole("button", { name: "View latest run for api-test-suite", exact: true })
        .boundingBox();

      expect(allRuns!.y).toBeCloseTo(latestRun!.y, 0);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      await page.mouse.click(8, 8);
      await page.screenshot({ path: testInfo.outputPath("job-details.png") });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-jobs--long-identifiers&viewMode=story");
      await expect(page.getByRole("heading", { name: "release-validation", exact: true })).toBeVisible();
      await expectAttachedCopyField(page, "Job ID");

      await page.evaluate(() => {
        Object.defineProperty(navigator, "clipboard", {
          configurable: true,
          value: {
            writeText: async (text: string) => {
              document.documentElement.dataset.copiedIdentifier = text;
            }
          }
        });
      });

      await page.getByRole("button", { name: "Copy Job ID", exact: true }).click();
      expect(await page.evaluate(() => document.documentElement.dataset.copiedIdentifier)).toBe(
        "release-source-validation-".repeat(5)
      );

      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }
}

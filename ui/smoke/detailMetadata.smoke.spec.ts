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

      const definitionLink = page.getByRole("link", { name: /^View captured definition for/ });
      await expect(definitionLink).toBeVisible();
      expect((await definitionLink.boundingBox())!.height).toBeGreaterThanOrEqual(40);
      await expect(definitionLink).toHaveAttribute("href", "#run-definition-title");
      await expect(page.getByRole("button", { name: "Copy Job ID", exact: true })).toHaveCount(0);

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
      await definitionLink.click();
      await expect(
        page.getByRole("region", { name: "Submitted Definition", exact: true }).locator("details")
      ).toHaveAttribute("open", "");

      await expect(
        page.getByRole("region", { name: "Submitted Definition", exact: true }).locator("pre")
      ).toBeVisible();

      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-jobs--crud&viewMode=story");
      await page.getByText("api-test-suite", { exact: true }).click();
      await expect(page.getByRole("heading", { name: "api-test-suite", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Copy Job ID", exact: true })).toHaveCount(0);

      const repositoryLink = page.getByRole("region", { name: "Provenance", exact: true }).getByRole("link");
      expect((await repositoryLink.boundingBox())!.height).toBeGreaterThanOrEqual(40);

      const linkWidth = await repositoryLink.evaluate((element) => ({
        link: element.getBoundingClientRect().width,
        field: element.closest("dd")!.getBoundingClientRect().width
      }));

      expect(linkWidth.link).toBeCloseTo(linkWidth.field, 0);

      const source = page.locator('dl[aria-label="Repository source"]');
      const frame = page.getByLabel("Definition frame", { exact: true });
      const sourceBox = (await source.boundingBox())!;
      const frameBox = (await frame.boundingBox())!;
      if (width === 1280) expect(sourceBox.x).toBeGreaterThan(frameBox.x + frameBox.width);
      else expect(sourceBox.y).toBeGreaterThan(frameBox.y + frameBox.height);
      await expect(page.getByRole("region", { name: "Provenance", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "View all runs for api-test-suite", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "View latest run for api-test-suite", exact: true })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      await page.mouse.click(8, 8);
      await page.screenshot({ path: testInfo.outputPath("job-details.png") });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-jobs--long-identifiers&viewMode=story");
      await expect(page.getByRole("heading", { name: "release-validation", exact: true })).toBeVisible();
      await expectAttachedCopyField(page, "Commit");

      const commit = page.getByRole("button", { name: "Copy Commit", exact: true }).locator("..");
      await expect(commit.locator("code")).toHaveAttribute("title", "0123456789abcdef".repeat(4));
      await expect(commit).toContainText("0123456789ab");
      expect((await commit.boundingBox())!.width).toBeGreaterThanOrEqual(200);
      await expect(page.getByRole("button", { name: "Copy Job ID", exact: true })).toHaveCount(0);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }
}

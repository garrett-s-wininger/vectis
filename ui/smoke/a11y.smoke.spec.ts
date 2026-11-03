import { contrastRatio, visibleTextContrast } from "./contrast";
import { writeFile } from "node:fs/promises";
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const paths = [
  "/jobs",
  "/jobs/create",
  "/jobs/job-api-test-suite",
  "/jobs/job-api-test-suite/config",
  "/runs",
  "/runs/run-1240",
  "/health",
  "/health/cell-local",
  "/users",
  "/users/user-admin",
  "/namespaces",
  "/namespaces/2",
  "/namespaces/2/config",
  "/profile",
  "/login",
  "/setup",
  "/missing-page"
];

for (const colorScheme of ["light", "dark"] as const) {
  for (const path of paths) {
    test(`WCAG AA ${colorScheme} ${path}`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.goto(path);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();

      await page.evaluate(() => document.fonts.ready);
      await page.screenshot({ path: testInfo.outputPath(`${colorScheme}-top.png`) });
      const top = await visibleTextContrast(page);
      await page.evaluate(() => {
        const main = document.querySelector("main");
        if (main) main.scrollTop = main.scrollHeight;

        window.scrollTo(0, document.body.scrollHeight);
      });

      await page.screenshot({ path: testInfo.outputPath(`${colorScheme}-bottom.png`) });
      const bottom = await visibleTextContrast(page);
      const renderedText = {
        checked: top.checked + bottom.checked,
        skippedOpacity: top.skippedOpacity + bottom.skippedOpacity,
        failures: [...top.failures, ...bottom.failures]
      };

      const results = await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
        .analyze();

      const summarize = (items: typeof results.violations) =>
        items.map((item) => ({
          id: item.id,
          impact: item.impact,
          help: item.help,
          helpUrl: item.helpUrl,
          nodes: item.nodes.map((node) => ({
            target: node.target,
            summary: node.failureSummary,
            messages: [...node.any, ...node.all, ...node.none].map((check) => check.message)
          }))
        }));

      const report = {
        path,
        colorScheme,
        renderedText,
        violations: summarize(results.violations),
        reviewNeeded: summarize(results.incomplete)
      };

      const reportPath = testInfo.outputPath("accessibility-report.json");
      await writeFile(reportPath, JSON.stringify(report, null, 2));
      await testInfo.attach("accessibility-report", {
        path: reportPath,
        contentType: "application/json"
      });

      expect(renderedText.checked).toBeGreaterThan(0);
      expect(renderedText.failures, JSON.stringify(renderedText.failures, null, 2)).toEqual([]);
      expect(report.reviewNeeded.filter((item) => item.id !== "color-contrast")).toEqual([]);
      expect(report.violations, JSON.stringify(report.violations, null, 2)).toEqual([]);
    });
  }
}

for (const colorScheme of ["light", "dark"] as const) {
  for (const [path, label] of [
    ["/jobs/create", "Name"],
    ["/profile", "Theme"],
    ["/health", "Inspect local"],
    ["/runs/run-1240", "Job routes: Running"]
  ]) {
    test(`control and focus contrast ${colorScheme} ${label}`, async ({ page }) => {
      await page.emulateMedia({ colorScheme });
      await page.goto(path);
      const control = label === "Theme" ? page.getByLabel(/^Theme: /) : page.getByLabel(label, { exact: true });
      await expect(control).toBeVisible();

      await control.focus();
      await page.keyboard.press("Tab");
      await page.keyboard.press("Shift+Tab");
      const colors = await control.evaluate((element) => {
        const style = getComputedStyle(element);
        const probe = document.createElement("span");
        document.body.append(probe);

        const surfaces = ["--surface-panel", "--surface-subtle", "--color-bg-metal-strong"].map((name) => {
          probe.style.color = `var(${name})`;
          return getComputedStyle(probe).color;
        });

        probe.remove();

        return {
          border: style.borderTopColor,
          background: style.backgroundColor,
          focus: style.outlineColor,
          width: parseFloat(style.outlineWidth),
          visible: element.matches(":focus-visible"),
          surfaces
        };
      });

      const rgb = (color: string) =>
        color
          .match(/[\d.]+/g)!
          .slice(0, 3)
          .map(Number);

      expect(colors.visible).toBe(true);
      expect(colors.width).toBeGreaterThan(0);

      for (const surface of [
        ...(colors.background === "rgba(0, 0, 0, 0)" ? [] : [colors.background]),
        ...colors.surfaces
      ]) {
        expect(contrastRatio(rgb(colors.border), rgb(surface)), `border against ${surface}`).toBeGreaterThanOrEqual(3);
        expect(contrastRatio(rgb(colors.focus), rgb(surface)), `focus against ${surface}`).toBeGreaterThanOrEqual(3);
      }
    });
  }
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`single local cell ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    await page.goto(
      "http://127.0.0.1:6101/iframe.html?id=components-data-executiontopology--single-healthy-cell&viewMode=story"
    );

    await expect(page.getByRole("button", { name: "Inspect local" })).toBeVisible();
    await page.evaluate(() => document.fonts.ready);

    const insets = await page.getByText("Route configured", { exact: true }).evaluate((element) => {
      const range = document.createRange();
      range.selectNodeContents(element);

      const text = range.getBoundingClientRect();
      const clip = element.getBoundingClientRect();

      return { top: text.top - clip.top, bottom: clip.bottom - text.bottom };
    });

    expect(insets.top).toBeGreaterThanOrEqual(-0.5);
    expect(insets.bottom).toBeGreaterThanOrEqual(-0.5);

    await page.screenshot({ path: testInfo.outputPath(`${colorScheme}.png`) });
    expect((await visibleTextContrast(page)).failures).toEqual([]);
  });
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`interactive colors ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    const check = async (state: string) => {
      await page.evaluate(() => document.fonts.ready);
      await page.screenshot({ path: testInfo.outputPath(`${state}.png`) });
      const result = await visibleTextContrast(page);

      expect(result.checked, state).toBeGreaterThan(0);
      expect(result.failures, `${state}: ${JSON.stringify(result.failures)}`).toEqual([]);
    };

    await page.goto("/users");
    await page.getByRole("button", { name: "Create", exact: true }).click();
    await page.getByRole("button", { name: "Add User" }).hover();
    await check("primary-hover");

    await page.getByRole("button", { name: "Cancel", exact: true }).hover();
    await check("quiet-hover");

    await page.getByLabel(/^Role: /).click();
    await check("role-menu");

    await page.goto("/users/user-mira");
    await page.getByRole("button", { name: "Remove User", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Remove User", exact: true }).hover();
    await check("danger-dialog-hover");

    await page.goto("/profile");
    await page.getByLabel(/^Theme: /).click();
    await check("theme-menu");

    await page.goto("http://127.0.0.1:6101/iframe.html?id=components-feedback-erroralert--default&viewMode=story");
    await expect(page.getByRole("alert")).toBeVisible();
    await check("error-alert");
  });
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`log output preserves lines and contrast ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-runlogoutput--mixed-streams&viewMode=story");
    const output = page.getByLabel("Log output");
    await expect(output).toBeVisible({ timeout: 20_000 });
    expect(await output.textContent()).toBe("Starting task\nhello\nExpected demo warning\nFinished\n");

    await page.evaluate(() => document.fonts.ready);
    await page.screenshot({ path: testInfo.outputPath(`${colorScheme}.png`) });
    const contrast = await visibleTextContrast(page);
    expect(contrast.checked).toBeGreaterThan(0);
    expect(contrast.failures).toEqual([]);
  });
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`log output overflow scrolls inside its border ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-runlogoutput--overflow&viewMode=story");
    const output = page.getByLabel("Log output");
    await expect(output).toBeVisible();

    await output.focus();
    await output.press("Control+End");
    await expect
      .poll(() =>
        output.evaluate((element) => Math.abs(element.scrollHeight - element.clientHeight - element.scrollTop))
      )
      .toBeLessThanOrEqual(1);

    const geometry = await output.evaluate((element) => {
      const frame = element.parentElement!;
      const rect = element.getBoundingClientRect();
      const bounds = frame.getBoundingClientRect();
      return {
        overflows: element.scrollHeight > element.clientHeight,
        inside: rect.top > bounds.top && rect.bottom < bounds.bottom,
        clipped: getComputedStyle(frame).overflow,
        scrollbar: getComputedStyle(element).scrollbarColor
      };
    });

    expect(geometry.overflows).toBe(true);
    expect(geometry.inside).toBe(true);
    expect(geometry.clipped).toBe("hidden");
    expect(geometry.scrollbar).not.toBe("auto");

    await page.screenshot({ path: testInfo.outputPath(`${colorScheme}-overflow.png`) });
    expect((await visibleTextContrast(page)).failures).toEqual([]);
  });
}

import { openNavigation } from "./navigationHelpers";
import { expect, test } from "@playwright/test";

async function expectNonBlankPage() {
  const visibleText = document.body.innerText.trim();
  const bodyBox = document.body.getBoundingClientRect();

  return {
    bodyHeight: bodyBox.height,
    bodyWidth: bodyBox.width,
    textLength: visibleText.length
  };
}

async function expectDropdownUsesOverlaySafePanel(
  page: import("@playwright/test").Page,
  summaryLabel: string,
  options: { crossesPanelBoundary?: boolean } = {}
) {
  await page.locator(`summary[aria-label="${summaryLabel}"]`).click();

  const geometry = await page.evaluate(() => {
    const openDropdown = document.querySelector<HTMLDetailsElement>('details[data-vectis-dropdown="true"][open]');
    const summary = openDropdown?.querySelector<HTMLElement>("summary");
    const menu = openDropdown?.querySelector<HTMLElement>('[class*="menu"]');
    const hostPanel = openDropdown?.closest<HTMLElement>(".polished-panel");
    const summaryBox = summary?.getBoundingClientRect();
    const menuBox = menu?.getBoundingClientRect();
    const hostBox = hostPanel?.getBoundingClientRect();
    const hostStyle = hostPanel ? getComputedStyle(hostPanel) : null;

    return {
      hostOverflow: hostStyle?.overflow,
      hostUsesOverlayContract: hostPanel?.classList.contains("polished-panel--overlay-safe") ?? false,
      hostBottom: hostBox?.bottom ?? 0,
      menuBottom: menuBox?.bottom ?? 0,
      summaryBottom: summaryBox?.bottom ?? 0
    };
  });

  expect(geometry.hostUsesOverlayContract).toBe(true);
  expect(geometry.hostOverflow).toBe("visible");
  expect(geometry.menuBottom).toBeGreaterThan(geometry.summaryBottom);

  if (options.crossesPanelBoundary) {
    expect(geometry.menuBottom).toBeGreaterThan(geometry.hostBottom);
  }
}

test.describe("app smoke", () => {
  test("shows the fanout handoff and respects reduced motion", async ({ page }, testInfo) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--fanout-handoff&viewMode=story");
    const graph = page.getByRole("group", { name: "Execution graph", exact: true });
    await expect(graph).toBeVisible({ timeout: 20000 });
    await expect(graph.getByRole("button", { name: "Fanout: Succeeded" })).toBeVisible();

    const waiting = graph.getByRole("status");
    await expect(waiting).toHaveText("Waiting for task updates");

    const animation = await waiting.locator("svg").evaluate((element) => getComputedStyle(element).animationName);
    expect(animation).toBe("none");

    await page.screenshot({ path: testInfo.outputPath("fanout-handoff.png") });
  });

  test("renders the cluster health page", async ({ page }) => {
    await page.goto("/health");

    await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();
    const accents = await page.locator("article").evaluateAll((cards) =>
      cards.map((card) => {
        const accent = getComputedStyle(card, "::before");
        return { height: parseFloat(accent.height), background: accent.backgroundImage };
      })
    );

    expect(accents).toHaveLength(3);

    for (const accent of accents) {
      expect(accent.height).toBeGreaterThan(0);
      expect(accent.background).toContain("linear-gradient");
    }

    await expect(page.getByRole("navigation", { name: "Primary" })).toContainText("Jobs");
    await expect(page.getByRole("region", { name: "Cells" })).toContainText("Gateway");

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
    expect(pageShape.bodyWidth).toBeGreaterThan(300);
    expect(pageShape.bodyHeight).toBeGreaterThan(300);
  });

  test("renders the jobs page with CRUD affordances", async ({ page }) => {
    await page.goto("/jobs?namespace=%2Fteam-a");

    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Create" })).toBeVisible();
    await expect(page.getByRole("table")).toContainText("Job");
    await openNavigation(page);
    await expect(page.getByLabel("Namespace: /team-a", { exact: true })).toBeVisible();

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
  });

  test("renders app dropdown menus outside polished panel boundaries", async ({ page }) => {
    await page.goto("/runs");
    await expect(page.getByRole("heading", { exact: true, name: "Runs" })).toBeVisible();
    await expectDropdownUsesOverlaySafePanel(page, "Status: All", { crossesPanelBoundary: true });

    await page.goto("/jobs/create");
    await expect(page.getByRole("heading", { name: "Create Job" })).toBeVisible();
    await expectDropdownUsesOverlaySafePanel(page, "Repository ID: vectis-local");

    await page.goto("/runs/run-1240");
    await expect(page.getByRole("heading", { name: "Run Logs" })).toBeVisible();
    await expect(page.getByLabel(/^Attempt:/)).toHaveCount(0);

    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--shared-attempt-context&viewMode=story");
    await expectDropdownUsesOverlaySafePanel(page, "Attempt: Latest (attempt 2)");
  });
});

test.describe("storybook smoke", () => {
  for (const width of [1280, 820, 390]) {
    test(`aligns run metadata across varied names and statuses at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=components-data-runlist--varied-content&viewMode=story");
      await expect(page.getByRole("heading", { name: "Runs with varied content" })).toBeVisible({ timeout: 20000 });

      const rows = page.getByRole("listitem");
      await expect(rows).toHaveCount(4);

      const subtitle = page.getByText("Run #2 · e31b23da7469", { exact: true });
      await expect(subtitle).toBeVisible();

      const labelSize = await subtitle.evaluate((element) => ({
        width: element.clientWidth,
        contentWidth: element.scrollWidth,
        height: element.getBoundingClientRect().height,
        lineHeight: Number.parseFloat(getComputedStyle(element).lineHeight)
      }));

      expect(labelSize.contentWidth).toBeLessThanOrEqual(labelSize.width);
      expect(labelSize.height).toBeLessThanOrEqual(labelSize.lineHeight + 1);

      const positions = await rows.evaluateAll((items) =>
        items.map((item) => ({
          right: item.getBoundingClientRect().right,
          facts: Array.from(item.querySelectorAll("dl > div")).map((fact) => {
            const box = fact.getBoundingClientRect();
            return { x: box.x, width: box.width, right: box.right };
          })
        }))
      );

      for (const row of positions) {
        for (const [index, fact] of row.facts.entries()) {
          expect(Math.abs(fact.x - positions[0].facts[index].x)).toBeLessThan(1);
          expect(Math.abs(fact.width - positions[0].facts[index].width)).toBeLessThan(1);
          expect(fact.right).toBeLessThanOrEqual(row.right);
        }
      }

      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }

  test("renders representative component and design stories", async ({ page }) => {
    await page.goto("http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--dashboard&viewMode=story");

    await expect(page.getByRole("heading", { name: "Cluster dashboard" })).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Administration" })).toContainText("Health");

    let pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
    expect(pageShape.bodyWidth).toBeGreaterThan(300);

    await page.goto("http://127.0.0.1:6101/iframe.html?id=design-tokens--inventory&viewMode=story");

    await expect(page.getByRole("heading", { name: "Colors" })).toBeVisible();
    await expect(page.getByText("--surface-page", { exact: true })).toBeVisible();

    pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
  });
});

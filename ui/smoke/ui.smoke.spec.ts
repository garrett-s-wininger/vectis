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
  test("renders the cluster health page", async ({ page }) => {
    await page.goto("/health");

    await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Primary" })).toContainText("Jobs");
    await expect(page.getByRole("region", { name: "Cells" })).toContainText("Gateway");

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
    expect(pageShape.bodyWidth).toBeGreaterThan(300);
    expect(pageShape.bodyHeight).toBeGreaterThan(300);
  });

  test("renders the jobs page with CRUD affordances", async ({ page }) => {
    await page.goto("/jobs");

    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Create" })).toBeVisible();
    await expect(page.getByRole("table")).toContainText("Job");
    await expect(page.getByLabel("Namespace")).toBeVisible();

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
  });

  test("renders app dropdown menus outside polished panel boundaries", async ({ page }) => {
    await page.goto("/runs");
    await expect(page.getByRole("heading", { exact: true, name: "Runs" })).toBeVisible();
    await expectDropdownUsesOverlaySafePanel(page, "Status: All", { crossesPanelBoundary: true });

    await page.goto("/jobs/create");
    await expect(page.getByRole("heading", { name: "Create Job" })).toBeVisible();
    await expectDropdownUsesOverlaySafePanel(page, "Cadence: None");
  });
});

test.describe("storybook smoke", () => {
  test("renders representative component and design stories", async ({ page }) => {
    await page.goto("http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--dashboard&viewMode=story");

    await expect(page.getByRole("heading", { name: "Cluster dashboard" })).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Utility" })).toContainText("Health");

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

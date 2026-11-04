import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const nested = "/teams/platform/build/east/nightly";
for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 820, 1280]) {
    test(`sidebar hierarchy ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 950 });
      await page.goto(
        "http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--nested-namespaces&viewMode=story"
      );

      await expect(page.getByRole("heading", { name: "Cluster dashboard", exact: true })).toBeVisible({
        timeout: 20000
      });

      expect((await page.getByRole("banner").boundingBox())!.height).toBe(56);

      if (width <= 900) {
        const logo = page.getByRole("banner").locator("img");
        const logoBounds = (await logo.boundingBox())!;
        expect(logoBounds.width).toBe(logoBounds.height);
        expect(logoBounds.width).toBe(28);
        await expect(page.getByRole("navigation", { name: "Primary", exact: true })).toHaveCount(0);

        await page.getByRole("button", { name: "Open navigation", exact: true }).click();
        await expect(page.getByRole("dialog", { name: "Navigation", exact: true })).toBeVisible();
        await expect(page.getByRole("button", { name: "Close navigation", exact: true })).toBeFocused();
      }

      const trigger = page.getByRole("button", { name: `Namespace: ${nested}`, exact: true });
      await trigger.click();
      await expect(page.getByRole("textbox", { name: "Search namespaces", exact: true })).toBeFocused();

      const leaf = page.getByRole("treeitem", { name: nested, exact: true });
      await expect(leaf).toHaveAttribute("aria-level", "6");
      await expect(leaf).toHaveAttribute("aria-selected", "true");

      const treeBounds = (await page.getByRole("tree", { name: "Namespaces", exact: true }).boundingBox())!;
      const selectedBounds = (await leaf.boundingBox())!;
      expect(selectedBounds.y).toBeGreaterThanOrEqual(treeBounds.y);
      expect(selectedBounds.y + selectedBounds.height).toBeLessThanOrEqual(treeBounds.y + treeBounds.height);

      await expect(page.getByLabel("Effective access: Operator", { exact: true })).toBeVisible();
      await expect(page.getByRole("link", { name: "Manage namespaces", exact: true })).toBeVisible();
      await expect(page.getByRole("link", { name: "View namespace", exact: true })).toHaveCSS(
        "text-decoration-line",
        "underline"
      );

      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);

      await page.screenshot({ path: testInfo.outputPath("namespace-hierarchy.png") });
      await page.keyboard.press("Escape");
      await expect(trigger).toBeFocused();

      if (width <= 900) {
        await expect(page.getByRole("dialog", { name: "Navigation", exact: true })).toBeVisible();

        await page.keyboard.press("Escape");
        await expect(page.getByRole("dialog", { name: "Navigation", exact: true })).toHaveCount(0);
        await expect(page.getByRole("button", { name: "Open navigation", exact: true })).toBeFocused();
      } else {
        const collapse = page.getByRole("button", { name: "Collapse navigation", exact: true });
        expect((await collapse.boundingBox())!.y).toBeGreaterThan(850);
        await expect(
          page.getByRole("banner").getByRole("button", { name: "Collapse navigation", exact: true })
        ).toHaveCount(0);

        await collapse.click();
        expect(
          (await page.getByRole("complementary", { name: "Application navigation", exact: true }).boundingBox())!.width
        ).toBe(72);

        await expect(page.getByRole("link", { name: "Jobs", exact: true })).toBeVisible();

        await page.reload();
        await expect(page.getByRole("button", { name: "Expand navigation", exact: true })).toBeVisible();

        await trigger.click();
        expect(
          (await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations
        ).toEqual([]);

        const panel = page.getByRole("tree", { name: "Namespaces", exact: true });
        expect((await panel.boundingBox())!.x).toBeGreaterThan(72);

        await page.screenshot({ path: testInfo.outputPath("collapsed-sidebar.png") });
        await page.keyboard.press("Escape");
        await page.getByRole("button", { name: "Expand navigation", exact: true }).click();
      }

      await page.screenshot({ path: testInfo.outputPath("sidebar.png") });
    });
  }
}

test("namespace search selects a deep path without losing context and has no overflow", async ({ page }) => {
  await page.goto("http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--nested-namespaces&viewMode=story");
  await page.getByRole("button", { name: `Namespace: ${nested}`, exact: true }).click();
  const search = page.getByRole("textbox", { name: "Search namespaces", exact: true });
  await search.fill("edge/production");
  await search.press("ArrowDown");
  const result = page.getByRole("treeitem", { name: "/teams/edge/production", exact: true });
  await expect(result).toBeFocused();

  await result.press("Enter");
  await expect(page.getByRole("button", { name: "Namespace: /teams/edge/production", exact: true })).toBeVisible();
  await expect(page.getByRole("tree")).toHaveCount(0);
});

test("mobile navigation traps focus and returns it after outside dismissal", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 850 });
  await page.goto("/jobs");
  await expect(page.getByRole("heading", { name: "Jobs", exact: true })).toBeVisible();

  const open = page.getByRole("button", { name: "Open navigation", exact: true });
  await open.click();
  const drawer = page.getByRole("dialog", { name: "Navigation", exact: true });
  const first = drawer.getByRole("link", { name: "Vectis home", exact: true });
  const last = drawer.getByRole("link", { name: "Users", exact: true });
  await first.focus();
  await page.keyboard.press("Shift+Tab");
  await expect(last).toBeFocused();

  await page.keyboard.press("Tab");
  await expect(first).toBeFocused();

  await page.mouse.click(380, 400);
  await expect(drawer).toHaveCount(0);
  await expect(open).toBeFocused();

  await open.click();
  await page.setViewportSize({ width: 1280, height: 850 });
  await expect(page.getByRole("dialog", { name: "Navigation", exact: true })).toHaveCount(0);
  await expect(page.getByRole("navigation", { name: "Primary", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Jobs", exact: true })).toBeVisible();
});

for (const width of [390, 1280]) {
  test(`namespace popout stays reachable in a short ${width}px window`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 400 });
    await page.goto(
      "http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--nested-namespaces&viewMode=story"
    );

    await expect(page.getByRole("heading", { name: "Cluster dashboard", exact: true })).toBeVisible({ timeout: 20000 });

    if (width <= 900) await page.getByRole("button", { name: "Open navigation", exact: true }).click();
    await page.getByRole("button", { name: `Namespace: ${nested}`, exact: true }).click();
    const panel = page.getByRole("tree", { name: "Namespaces", exact: true }).locator("..");
    const bounds = (await panel.boundingBox())!;
    expect(bounds.y).toBeGreaterThanOrEqual(0);
    expect(bounds.y + bounds.height).toBeLessThanOrEqual(400);

    const manage = page.getByRole("link", { name: "Manage namespaces", exact: true });
    await manage.scrollIntoViewIfNeeded();
    await expect(manage).toBeInViewport();

    const search = page.getByRole("textbox", { name: "Search namespaces", exact: true });
    await search.fill("missing-namespace");
    await expect(page.getByRole("status")).toHaveText("No matching namespaces.");
    expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
      []
    );

    await page.screenshot({ path: testInfo.outputPath("short-namespace-popout.png") });
  });
}

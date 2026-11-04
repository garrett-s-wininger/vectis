import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Locator, type Page } from "@playwright/test";
import { contrastRatio, visibleTextContrast } from "./contrast";

const rgb = (color: string) =>
  color
    .match(/[\d.]+/g)!
    .slice(0, 3)
    .map(Number);

async function controlContrast(page: Page, control: Locator) {
  const styles = await control.evaluate((element) => {
    const style = getComputedStyle(element);
    const bounds = element.getBoundingClientRect();
    return {
      border: style.borderLeftColor,
      borderWidth: parseFloat(style.borderLeftWidth),
      focus: element.matches(":focus-visible") ? style.outlineColor : undefined,
      focusWidth: parseFloat(style.outlineWidth),
      x: bounds.left,
      y: bounds.top + bounds.height / 2
    };
  });

  const png = (await page.screenshot({ animations: "disabled", caret: "hide" })).toString("base64");
  const surfaces = await page.evaluate(
    async ({ png, x, y }) => {
      const image = await createImageBitmap(
        new Blob([Uint8Array.from(atob(png), (value) => value.charCodeAt(0))], { type: "image/png" })
      );

      const canvas = document.createElement("canvas");
      canvas.width = image.width;
      canvas.height = image.height;

      const context = canvas.getContext("2d")!;
      context.drawImage(image, 0, 0);

      const scale = image.width / innerWidth;
      return [-7, 6].map((offset) =>
        Array.from(context.getImageData(Math.floor((x + offset) * scale), Math.floor(y * scale), 1, 1).data).slice(0, 3)
      );
    },
    { png, x: styles.x, y: styles.y }
  );

  expect(styles.borderWidth).toBeGreaterThan(0);

  const borders = surfaces.map((surface) => contrastRatio(rgb(styles.border), surface));
  for (const ratio of borders) expect(ratio, "control border against adjacent surface").toBeGreaterThanOrEqual(3);

  const focus = styles.focus ? surfaces.map((surface) => contrastRatio(rgb(styles.focus!), surface)) : [];
  if (styles.focus) {
    expect(styles.focusWidth).toBeGreaterThan(0);
    for (const ratio of focus) expect(ratio, "keyboard focus against adjacent surface").toBeGreaterThanOrEqual(3);
  }

  return { borders, focus };
}

async function filledHoverContrast(control: Locator) {
  await control.hover();
  const colors = await control.evaluate((element) => {
    const style = getComputedStyle(element);
    const probe = document.createElement("span");
    document.body.append(probe);

    const surfaces = [
      "--surface-navigation",
      "--surface-navigation-recessed",
      "--surface-topbar",
      "--surface-topbar-raised",
      "--surface-panel",
      "--surface-subtle"
    ].map((name) => {
      probe.style.color = `var(${name})`;
      return getComputedStyle(probe).color;
    });

    probe.remove();
    return { ink: style.color, fill: style.backgroundColor, surfaces };
  });

  const text = contrastRatio(rgb(colors.ink), rgb(colors.fill));
  expect(text, "hover icon and label contrast").toBeGreaterThanOrEqual(4.5);

  const boundaries = colors.surfaces.map((surface) => contrastRatio(rgb(colors.fill), rgb(surface)));
  for (const ratio of boundaries) expect(ratio, "hover fill against navigation surfaces").toBeGreaterThanOrEqual(3);

  return { text, boundaries };
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`shell and navigation control contrast ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    await page.goto(
      "http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--nested-namespaces&viewMode=story"
    );

    await expect(page.getByRole("heading", { name: "Cluster dashboard", exact: true })).toBeVisible({ timeout: 20000 });
    const report: Record<string, unknown> = {};
    const shell = await page.getByRole("banner").evaluate((element) => {
      const style = getComputedStyle(element);
      const probe = document.createElement("span");
      probe.style.color = "var(--surface-page)";
      document.body.append(probe);

      const page = getComputedStyle(probe).color;
      probe.remove();
      const navigation = getComputedStyle(document.querySelector("aside")!).backgroundColor;
      return { background: style.backgroundColor, navigation, page };
    });

    expect(new Set([shell.background, shell.navigation, shell.page]).size).toBe(3);

    report.surfaces = shell;
    async function check(name: string, control: Locator) {
      report[`${name}:default`] = await controlContrast(page, control);
      expect((await visibleTextContrast(page)).failures).toEqual([]);

      await control.hover();
      report[`${name}:hover`] = await controlContrast(page, control);
      expect((await visibleTextContrast(page)).failures).toEqual([]);

      await page.mouse.move(0, 0);
      await control.focus();
      await page.keyboard.press("Tab");
      await page.keyboard.press("Shift+Tab");
      const focused = await controlContrast(page, control);
      expect(focused.focus).toHaveLength(2);

      report[`${name}:focus`] = focused;
      expect((await visibleTextContrast(page)).failures).toEqual([]);
    }

    await check("account", page.getByLabel("Account menu for admin", { exact: true }));
    await check(
      "namespace",
      page.getByRole("button", { name: "Namespace: /teams/platform/build/east/nightly", exact: true })
    );

    await page.getByRole("button", { name: "Namespace: /teams/platform/build/east/nightly", exact: true }).click();
    await check("view namespace", page.getByRole("link", { name: "View namespace", exact: true }));
    await check("manage namespaces", page.getByRole("link", { name: "Manage namespaces", exact: true }));

    report.search = await controlContrast(
      page,
      page.getByRole("textbox", { name: "Search namespaces", exact: true }).locator("..")
    );

    expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
      []
    );

    await page.screenshot({ path: testInfo.outputPath("namespace-controls.png") });
    report.branchHover = await filledHoverContrast(
      page.getByRole("button", { name: "Collapse /teams/platform", exact: true })
    );

    await page.screenshot({ path: testInfo.outputPath("branch-hover.png") });
    await page.keyboard.press("Escape");
    const collapse = page.getByRole("button", { name: "Collapse navigation", exact: true });
    report.collapseHover = await filledHoverContrast(collapse);
    expect((await visibleTextContrast(page)).failures).toEqual([]);

    await page.screenshot({ path: testInfo.outputPath("sidebar-hover.png") });
    await collapse.click();
    const expand = page.getByRole("button", { name: "Expand navigation", exact: true });
    report.expandHover = await filledHoverContrast(expand);

    await page.screenshot({ path: testInfo.outputPath("collapsed-framing.png") });
    await expand.click();
    await page.getByLabel("Account menu for admin", { exact: true }).click();
    expect((await visibleTextContrast(page)).failures).toEqual([]);

    await page.screenshot({ path: testInfo.outputPath("account-control.png") });
    await testInfo.attach("contrast-ratios", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json"
    });
  });
}

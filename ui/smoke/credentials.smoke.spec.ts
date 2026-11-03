import { expect, test } from "@playwright/test";

for (const width of [390, 1280]) {
  test(`initial password fits its dialog and copies in full at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-users--initial-password&viewMode=story");
    await page.getByRole("button", { name: "Create", exact: true }).click();
    await page.getByLabel("Username", { exact: true }).fill("demo-user");
    await page.getByRole("button", { name: "Add User", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Initial Password" });
    await expect(dialog).toBeVisible();

    const password = dialog.getByLabel("Initial password for demo-user", { exact: true });
    const copy = dialog.getByRole("button", { name: "Copy initial password for demo-user", exact: true });
    await expect(copy.locator("svg")).toBeVisible();
    await expect(copy).toBeFocused();

    await page.keyboard.press("Shift+Tab");
    await expect(dialog.getByRole("button", { name: "Dismiss", exact: true })).toBeFocused();

    await page.keyboard.press("Tab");
    await expect(copy).toBeFocused();

    const geometry = await password.evaluate((element) => {
      const value = element.getBoundingClientRect();
      const button = element.closest("[class*=root]")?.querySelector("button")?.getBoundingClientRect();
      const panel = element.closest('[role="dialog"]')!.getBoundingClientRect();
      return {
        value: { left: value.left, right: value.right },
        button: { left: button?.left ?? 0, right: button?.right ?? 0 },
        panel: { left: panel.left, right: panel.right },
        overflows: element.scrollWidth > element.clientWidth
      };
    });

    expect(geometry.overflows).toBe(false);
    expect(geometry.value.right).toBeLessThanOrEqual(geometry.button.left);
    expect(geometry.value.left).toBeGreaterThan(geometry.panel.left);
    expect(geometry.button.right).toBeLessThan(geometry.panel.right);
    expect(geometry.panel.left).toBeGreaterThanOrEqual(0);
    expect(geometry.panel.right).toBeLessThanOrEqual(width);

    const dismiss = dialog.getByRole("button", { name: "Dismiss", exact: true });
    const panelBox = await dialog.boundingBox();
    const dismissBox = await dismiss.boundingBox();
    expect(dismissBox!.width).toBeLessThan(panelBox!.width / 2);
    expect(dismissBox!.x + dismissBox!.width / 2).toBeCloseTo(panelBox!.x + panelBox!.width / 2, 0);

    const affordance = await copy.evaluate((element) => {
      const style = getComputedStyle(element);
      return {
        border: parseFloat(style.borderTopWidth),
        background: style.backgroundColor,
        width: element.getBoundingClientRect().width
      };
    });

    expect(affordance.border).toBeGreaterThanOrEqual(1);
    expect(affordance.background).not.toBe("rgba(0, 0, 0, 0)");
    expect(affordance.width).toBeGreaterThanOrEqual(32);

    await page.evaluate(() => {
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: async (value: string) => {
            document.documentElement.dataset.copiedCredential = value;
          }
        }
      });
    });

    await copy.click();
    await expect(dialog.getByText("initial password for demo-user copied.", { exact: true })).toHaveText(
      "initial password for demo-user copied."
    );

    expect(await page.evaluate(() => document.documentElement.dataset.copiedCredential)).toBe(
      "synthetic-long-initial-password-".repeat(5)
    );

    await page.screenshot({ path: testInfo.outputPath("initial-password.png") });
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
    await expect(page.getByRole("button", { name: "Create", exact: true })).toBeFocused();
  });
}

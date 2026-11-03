import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280]) {
    test(`profile password controls ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 900 });
      await page.route("**/ui/api/context", (route) =>
        route.fulfill({ json: { auth_enabled: true, principal: { kind: "local_user", username: "admin" } } })
      );

      await page.route("**/ui/api/logout", (route) => route.fulfill({ status: 204 }));
      await page.goto("/profile");
      await expect(page.getByLabel("Current password")).toBeVisible();
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      const button = page.getByRole("button", { name: "Change password", exact: true });
      const bounds = (await button.boundingBox())!;
      expect(bounds.x).toBeGreaterThanOrEqual(0);
      expect(bounds.x + bounds.width).toBeLessThanOrEqual(width);

      await page.screenshot({ path: testInfo.outputPath("profile.png") });
      await page.getByLabel("Current password", { exact: true }).fill("current-password");
      await page.getByLabel("New password", { exact: true }).fill("new-password");
      await page.getByLabel("Confirm new password", { exact: true }).fill("new-password");
      await button.click();
      await expect(page.getByRole("heading", { name: "Sign in", exact: true })).toBeVisible();
      await expect(page.getByRole("status")).toHaveText("Password changed. Sign in with your new password.");
      await expect(page.getByLabel("Password", { exact: true })).toHaveValue("");
    });
  }
}

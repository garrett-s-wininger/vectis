import { expect, test } from "@playwright/test";

for (const role of ["operator", "viewer"]) {
  test(`${role} sees the appropriate saved-job controls`, async ({ page }) => {
    await page.goto(`http://127.0.0.1:6101/iframe.html?id=pages-jobs--${role}&viewMode=story`);
    await expect(page.getByRole("heading", { name: "Jobs", exact: true })).toBeVisible({ timeout: 20000 });
    await expect(page.getByRole("button", { name: "Create", exact: true })).toHaveCount(0);

    await page.getByText("api-test-suite", { exact: true }).click();
    await expect(page.getByRole("heading", { name: "api-test-suite", exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: /^(Edit|Configure) api-test-suite$/ })).toHaveCount(0);

    const trigger = page.getByRole("button", { name: "Run api-test-suite", exact: true });
    if (role === "operator") await expect(trigger).toBeVisible();
    else await expect(trigger).toHaveCount(0);
  });

  test(`${role} sees the appropriate Run Once controls`, async ({ page }) => {
    await page.goto(`http://127.0.0.1:6101/iframe.html?id=pages-runs--${role}&viewMode=story`);
    await expect(page.getByRole("heading", { name: "Runs", exact: true })).toBeVisible({ timeout: 20000 });

    const runOnce = page.getByRole("button", { name: "Run Once", exact: true });
    if (role === "viewer") {
      await expect(runOnce).toHaveCount(0);
      return;
    }

    await runOnce.click();
    await expect(page.getByRole("button", { name: "Submit run", exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Submit run", exact: true }).click();
    await expect(page.getByRole("button", { name: "Run Once", exact: true })).toBeVisible();
  });
}

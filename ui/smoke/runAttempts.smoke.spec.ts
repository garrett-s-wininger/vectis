import { expect, test } from "@playwright/test";

test.use({ screenshot: "only-on-failure" });

for (const colorScheme of ["light", "dark"] as const) {
  test(`retained task attempts show their own execution and outcome ${colorScheme}`, async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.emulateMedia({ colorScheme });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--retried-run&viewMode=story");
    const logs = page.getByRole("region", { name: "Task Logs", exact: true });
    await expect(logs.getByText("exec-latest", { exact: true })).toBeVisible({ timeout: 15_000 });

    await logs.getByLabel(/^Attempt: /).click();
    await page.getByRole("listbox").getByRole("option", { name: "Attempt 1", exact: true }).click();
    await expect(logs.getByText("exec-old", { exact: true })).toBeVisible();
    await expect(logs.getByText("Failed", { exact: true })).toBeVisible();
    await expect(logs.getByText("Succeeded", { exact: true })).toHaveCount(0);

    await logs.getByLabel(/^Attempt: /).click();
    await page.getByRole("listbox").getByRole("option", { name: "Attempt 2", exact: true }).click();
    await expect(logs.getByText("exec-latest", { exact: true })).toBeVisible();
    await expect(logs.getByText("Succeeded", { exact: true })).toBeVisible();

    await logs.getByLabel(/^Attempt: /).click();
    await page.getByRole("listbox").getByRole("option", { name: "All attempts", exact: true }).click();
    await expect(logs.locator("dd").filter({ hasText: /^All attempts$/ })).toBeVisible();
    expect(errors).toEqual([]);
  });
}

import { expect, test } from "@playwright/test";

for (const [theme, width] of [
  ["light", 1280],
  ["dark", 390]
] as const) {
  test(`run list filters and panel position survive investigation ${theme} ${width}px`, async ({ page }) => {
    await page.emulateMedia({ colorScheme: theme });
    await page.setViewportSize({ width, height: 420 });
    await page.goto("/runs?keep=yes");
    const job = page.getByRole("textbox", { name: "Job", exact: true });
    await expect(job).toBeVisible();

    await page.getByLabel("Namespace", { exact: true }).click();
    await page.getByRole("button", { name: "/prod", exact: true }).click();
    await page.getByLabel("Status: All", { exact: true }).click();
    await page.getByRole("listbox").getByRole("option", { name: "Succeeded", exact: true }).click();
    await page.getByLabel("Source: All", { exact: true }).click();
    await page.getByRole("listbox").getByRole("option", { name: "Saved", exact: true }).click();
    await page.goBack();
    await expect(page.getByLabel("Source: All", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Status: Succeeded", { exact: true })).toBeVisible();

    await page.goForward();
    await expect(page.getByLabel("Source: Saved", { exact: true })).toBeVisible();

    const entries = await page.evaluate(() => history.length);
    await job.pressSequentially("worker");
    await expect(job).toHaveValue("worker");
    expect(await page.evaluate(() => history.length)).toBe(entries);

    await expect.poll(() => new URL(page.url()).searchParams.get("status")).toBe("succeeded");
    expect(new URL(page.url()).searchParams.get("source")).toBe("stored");

    await page.reload();
    await expect(job).toHaveValue("worker");
    await expect(page.getByLabel("Namespace", { exact: true })).toContainText("/prod");
    await expect(page.getByLabel("Status: Succeeded", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Source: Saved", { exact: true })).toBeVisible();

    const main = page.getByRole("main");
    const listPosition = await main.evaluate((element) => {
      element.scrollTop = element.scrollHeight - element.clientHeight;
      return element.scrollTop;
    });

    expect(listPosition).toBeGreaterThan(0);

    await page.getByRole("button", { name: "Open run worker-image #1238", exact: true }).click();
    await expect(page.getByRole("heading", { level: 1 })).toHaveText("worker-image (#1238)");
    await expect.poll(() => main.evaluate((element) => element.scrollTop)).toBe(0);
    const detailPosition = await main.evaluate(
      (element) =>
        new Promise<number>((resolve) => {
          element.scrollTop = 200;
          requestAnimationFrame(() => resolve(element.scrollTop));
        })
    );

    await page.goBack();
    await expect(job).toHaveValue("worker");
    await expect.poll(() => main.evaluate((element) => element.scrollTop)).toBeCloseTo(listPosition, 0);

    await page.goForward();
    await expect(page.getByRole("heading", { level: 1 })).toHaveText("worker-image (#1238)");
    await expect.poll(() => main.evaluate((element) => element.scrollTop)).toBeCloseTo(detailPosition, 0);

    await page.getByRole("button", { name: "Build image: Succeeded", exact: true }).click();
    await page.reload();
    await expect(page.getByRole("button", { name: "Build image: Succeeded", exact: true })).toHaveAttribute(
      "aria-pressed",
      "true"
    );

    await page
      .getByRole("navigation", { name: "Run location" })
      .getByRole("button", { name: "Runs", exact: true })
      .click();
    await expect(job).toHaveValue("worker");
    await expect(page.getByLabel("Namespace", { exact: true })).toContainText("/prod");
    await expect(page.getByLabel("Status: Succeeded", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Source: Saved", { exact: true })).toBeVisible();
    await expect.poll(() => main.evaluate((element) => element.scrollTop)).toBeCloseTo(listPosition, 0);

    await page.getByRole("button", { name: "Open run worker-image #1238", exact: true }).click();
    await page
      .getByRole("navigation", { name: "Primary", exact: true })
      .getByRole("link", { name: "Runs", exact: true })
      .click();

    await expect(job).toHaveValue("worker");
    await expect.poll(() => main.evaluate((element) => element.scrollTop)).toBeCloseTo(listPosition, 0);

    await page.getByRole("button", { name: "Clear", exact: true }).click();
    await expect(page).toHaveURL(/\/runs\?keep=yes$/);

    await page.goBack();
    await expect(job).toHaveValue("worker");
    await expect(page.getByLabel("Namespace", { exact: true })).toContainText("/prod");
    await expect(page.getByLabel("Status: Succeeded", { exact: true })).toBeVisible();
  });
}

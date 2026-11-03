import { expect, test } from "@playwright/test";

for (const [theme, width] of [
  ["light", 1280],
  ["dark", 390]
] as const) {
  test(`run investigation links and browser history ${theme} ${width}px`, async ({ page }) => {
    await page.emulateMedia({ colorScheme: theme });
    await page.setViewportSize({ width, height: 900 });
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));

    await page.goto("/runs/run-1238?task=build&attempt=1&keep=yes#run-graph-title");
    const build = page.getByRole("button", { name: "Build image: Succeeded", exact: true });
    const scan = page.getByRole("button", { name: "Scan image: Succeeded", exact: true });
    const graph = page.getByRole("group", { name: "Execution graph", exact: true });
    await expect(build).toHaveAttribute("aria-pressed", "true");
    await expect(page.getByLabel(/^Attempt:/)).toHaveText("Attempt 1");
    await expect(page.getByRole("heading", { name: "Task Logs", exact: true })).toBeVisible();

    await graph.evaluate((element) => element.setAttribute("data-mount-marker", "original"));
    await scan.click();
    await expect.poll(() => new URL(page.url()).searchParams.get("task")).toBe("scan");
    await expect(scan).toHaveAttribute("aria-pressed", "true");
    await expect(graph).toHaveAttribute("data-mount-marker", "original");

    await page.goBack();
    await expect(build).toHaveAttribute("aria-pressed", "true");
    await expect(graph).toHaveAttribute("data-mount-marker", "original");

    await page.goForward();
    await expect(scan).toHaveAttribute("aria-pressed", "true");
    await expect(graph).toHaveAttribute("data-mount-marker", "original");

    await page.reload();
    await expect(scan).toHaveAttribute("aria-pressed", "true");
    await expect(page.getByLabel(/^Attempt:/)).toHaveText("Attempt 1");

    await page.getByLabel(/^Attempt:/).click();
    await page.getByRole("listbox").getByRole("option", { name: "Latest (attempt 1)", exact: true }).click();
    await expect.poll(() => new URL(page.url()).searchParams.get("attempt")).toBe(null);
    await expect(scan).toHaveAttribute("aria-pressed", "true");

    await page.getByRole("button", { name: "Clear task selection", exact: true }).click();
    await expect.poll(() => new URL(page.url()).searchParams.get("task")).toBe(null);
    await expect(page.getByRole("heading", { name: "Run Logs", exact: true })).toBeVisible();
    expect(new URL(page.url()).searchParams.get("keep")).toBe("yes");
    expect(new URL(page.url()).hash).toBe("#run-graph-title");
    expect(errors).toEqual([]);
  });
}

test("unavailable tasks and malformed attempts recover to the whole run", async ({ page }) => {
  await page.goto("/runs/run-1238?task=removed-task&attempt=-1");
  await expect(page.getByRole("region", { name: "Graph", exact: true }).getByRole("status")).toContainText(
    "Showing all tasks."
  );
  await expect(page.getByRole("heading", { name: "Run Logs", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Clear task selection", exact: true }).click();
  await expect(page).toHaveURL(/\/runs\/run-1238$/);
});

test("older full task-ID links remain valid and new selections use task keys", async ({ page }) => {
  await page.goto("/runs/run-1238?task=run-1238%3Abuild&attempt=1");
  await expect(page.getByRole("button", { name: "Build image: Succeeded", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true"
  );
  await page.getByLabel(/^Attempt:/).click();
  await page.getByRole("listbox").getByRole("option", { name: "Latest (attempt 1)", exact: true }).click();
  await expect(page).toHaveURL(/\/runs\/run-1238\?task=build$/);
  await page.goBack();
  await expect(page.getByRole("button", { name: "Build image: Succeeded", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true"
  );
});

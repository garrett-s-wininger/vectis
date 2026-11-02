import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.route("**/ui/api/context", (route) =>
    route.fulfill({ json: { auth_enabled: false, principal: { kind: "auth_disabled", username: "admin" } } })
  );
});

test("keeps known nodes and selected context visible while a task snapshot is incomplete", async ({ page }) => {
  await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--partial-task-updates&viewMode=story");
  const finish = page.getByRole("button", { name: "Finish: Running", exact: true });
  await expect(finish).toBeVisible({ timeout: 20000 });
  await expect(finish).toHaveAttribute("aria-pressed", "false");

  await finish.click();
  await expect(finish).toHaveAttribute("aria-pressed", "true");

  await page.getByRole("button", { name: "Receive partial update", exact: true }).click();
  await expect(page.getByText("Waiting for task updates", { exact: true })).toBeVisible();
  await expect(finish).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("region", { name: "Task Logs" })).toContainText("Finish");

  await page.getByRole("button", { name: "Receive complete update", exact: true }).click();
  await expect(page.getByText("Waiting for task updates", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Finish: Succeeded", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true"
  );
});

for (const width of [1280, 390]) {
  test(`holds the log viewport height as output arrives at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-runlogoutput--growing-output&viewMode=story");
    const output = page.getByLabel("Log output", { exact: true });
    await expect(output).toBeVisible({ timeout: 20000 });

    const after = page.getByText("Content after logs", { exact: true });
    const beforeHeight = (await output.boundingBox())!.height;
    const beforeY = (await after.boundingBox())!.y;
    for (const count of [1, 100]) {
      await page.getByRole("button", { name: "Append output", exact: true }).click();
      await expect(output.locator("span")).toHaveCount(count);
      expect((await output.boundingBox())!.height).toBe(beforeHeight);
      expect((await after.boundingBox())!.y).toBe(beforeY);
    }

    expect(await output.evaluate((element) => element.scrollHeight > element.clientHeight)).toBe(true);
    expect(await output.evaluate((element) => getComputedStyle(element).overflowY)).toBe("auto");
  });
}

test("keeps the final task selected while task completion precedes run completion", async ({ page }) => {
  await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--task-completion&viewMode=story");
  const finish = page.getByRole("button", { name: "Finish: Running", exact: true });
  await expect(finish).toBeVisible({ timeout: 20000 });
  await expect(finish).toHaveAttribute("aria-pressed", "false");

  await finish.click();
  await expect(finish).toHaveAttribute("aria-pressed", "true");

  await page.getByRole("button", { name: "Finish task", exact: true }).click();
  const completed = page.getByRole("button", { name: "Finish: Succeeded", exact: true });
  await expect(completed).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText("Waiting for task updates", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("region", { name: "Run summary" })).toContainText("Running");
  await expect(page.getByRole("region", { name: "Task Logs" })).toContainText("Finish");

  await page.getByRole("button", { name: "Finish run", exact: true }).click();
  await expect(page.getByRole("region", { name: "Run summary" })).toContainText("Succeeded");
  await expect(completed).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("button", { name: "root: Succeeded", exact: true })).toHaveAttribute(
    "aria-pressed",
    "false"
  );
});

test("a completed SSE log response ends cleanly in the browser", async ({ page }) => {
  const output = { stream: 0, sequence: 1, data: "Finished successfully.\n", execution_id: "root-execution" };
  const completion = { stream: 2, sequence: 2, data: "", completed: 1 };
  await page.route("**/api/v1/runs/protocol-completion/logs?tail=500", (route) =>
    route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body: `data: ${JSON.stringify(output)}\n\ndata: ${JSON.stringify(completion)}\n\n`
    })
  );

  await page.goto("/runs");
  const result = await page.evaluate(async () => {
    const path = "/src/data/runLogs.ts";
    const { openRunLogStream } = await import(path);
    return new Promise<{ status: string; output: string }>((resolve) => {
      let output = "";
      openRunLogStream(
        "protocol-completion",
        (entry: { data: string }) => {
          output += entry.data;
        },
        () => resolve({ status: "error", output }),
        () => resolve({ status: "complete", output })
      );
    });
  });

  expect(result).toEqual({ status: "complete", output: "Finished successfully.\n" });
});

test("an established SSE response closing without completion is distinguished from a failed connection", async ({
  page
}) => {
  await page.route("**/api/v1/runs/cancelled-stream/logs?tail=500", (route) =>
    route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body: `data: ${JSON.stringify({ stream: 0, sequence: 1, data: "Last output before cancellation.\n" })}\n\n`
    })
  );

  await page.goto("/runs");
  const result = await page.evaluate(async () => {
    const path = "/src/data/runLogs.ts";
    const { openRunLogStream } = await import(path);
    return new Promise<{ opened: boolean; output: string }>((resolve) => {
      let output = "";
      openRunLogStream(
        "cancelled-stream",
        (entry: { data: string }) => {
          output += entry.data;
        },
        (_message: string, opened: boolean) => resolve({ opened, output })
      );
    });
  });

  expect(result).toEqual({ opened: true, output: "Last output before cancellation.\n" });
});

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [1280, 390]) {
    test(`visible run identifiers and replay fact ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-run-detail--replay-metadata&viewMode=story");
      await expect(page.getByRole("heading", { level: 1, name: "Run once (#2)" })).toBeVisible({ timeout: 20000 });

      const breadcrumbs = page.getByRole("navigation", { name: "Run location" });
      await expect(breadcrumbs.locator(":scope > *")).toHaveText(["/team-a", "Runs", "e31b23da7469", "#2"]);
      await expect(breadcrumbs.getByText("e31b23da7469", { exact: true })).toBeVisible();

      expect(
        await breadcrumbs
          .getByText("e31b23da7469", { exact: true })
          .evaluate((element) => element.scrollWidth <= element.clientWidth)
      ).toBe(true);

      await expect(breadcrumbs.getByText("#2", { exact: true })).toBeVisible();

      const summary = page.getByRole("region", { name: "Run summary" });
      await expect(summary.getByText("95d24ba7-aef2-43cb-ae40-4fe6a63c2f82", { exact: true })).toBeVisible();
      await expect(summary.getByText("2a5ed99c-6d51-4c50-97e9-e31b23da7469", { exact: true })).toBeVisible();
      await expect(summary.getByRole("link", { name: /^View original run/ })).toHaveText("Run once #1");

      if (width > 1000) {
        const fields = await Promise.all(
          ["Run ID", "Job ID", "Re-run of"].map((label) =>
            summary.getByText(label, { exact: true }).locator("..").boundingBox()
          )
        );

        expect(fields.every((field) => field && field.y === fields[0]?.y)).toBe(true);
      }

      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
      await page.screenshot({ path: testInfo.outputPath("run-metadata.png") });
    });

    test(`header run controls and history ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 900 });
      await page.goto("/runs/run-1238");
      await expect(page.getByRole("button", { name: "Run again", exact: true })).toBeVisible();
      await page.evaluate(() => {
        document.documentElement.dataset.replayLoadingFlash = "false";
        new MutationObserver((records) => {
          if (
            records.some((record) =>
              Array.from(record.addedNodes).some((node) => node.textContent?.includes("Loading Run"))
            )
          ) {
            document.documentElement.dataset.replayLoadingFlash = "true";
          }
        }).observe(document.body, { childList: true, subtree: true });
      });

      await page.getByRole("button", { name: "Run again", exact: true }).click();
      await expect(page).toHaveURL(/\/runs\/run-1241$/);
      await expect(page.getByRole("link", { name: /^View original run/ })).toHaveAttribute("href", "/runs/run-1238");

      const summary = page.getByRole("region", { name: "Run summary" });
      await expect(summary).toContainText("worker-image #1238");
      await expect(summary.getByText("run-1241", { exact: true })).toBeVisible();
      await expect(page.locator("html")).toHaveAttribute("data-replay-loading-flash", "false");

      const heading = page.getByRole("heading", { level: 1 });
      await expect(heading).toHaveText("worker-image (#1239)");

      const cancel = page.getByRole("button", { name: "Cancel run", exact: true });
      await expect(heading.locator("xpath=ancestor::header")).toContainText("Cancel run");

      const before = await cancel.boundingBox();
      const graphBefore = await page.getByRole("region", { name: "Graph", exact: true }).boundingBox();
      await cancel.click();
      const confirmation = page.getByRole("button", { name: "Confirm cancel", exact: true });
      expect(await confirmation.boundingBox()).toEqual(before);
      expect(await page.getByRole("region", { name: "Graph", exact: true }).boundingBox()).toEqual(graphBefore);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      await page.screenshot({ path: testInfo.outputPath("run-controls.png") });
      await confirmation.click();
      await expect(page.getByRole("region", { name: "Run summary" })).toContainText("Cancelled");
      await expect(page.getByRole("button", { name: "Run again", exact: true })).toBeEnabled();

      await page.getByRole("link", { name: /^View original run/ }).click();
      await expect(page).toHaveURL(/\/runs\/run-1238$/);
      await expect(page.getByRole("heading", { level: 1 })).toHaveText("worker-image (#1238)");
      await expect(page.locator("html")).toHaveAttribute("data-replay-loading-flash", "false");
    });
  }
}

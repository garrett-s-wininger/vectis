import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

const operations = [
  ["get", "/api/v1/runs"],
  ["post", "/api/v1/jobs/run"],
  ["post", "/api/v1/tokens"],
  ["get", "/api/v1/runs/{id}/artifacts/{name}/download"],
  ["get", "/api/v1/sse/source-repositories/{id}/jobs/{job_id}/runs"],
  ["put", "/api/v1/users/{id}"],
  ["delete", "/api/v1/namespaces/{id}/bindings/{user_id}"],
  ["patch", "/api/v1/source-schedules/{schedule_id}"]
] as const;

for (const theme of ["light", "dark"] as const) {
  for (const width of [1280, 390]) {
    test(`reference operation families ${theme} ${width}px`, async ({ page }, testInfo) => {
      test.setTimeout(60000);
      const errors: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));

      await page.setViewportSize({ width, height: 1000 });
      await page.emulateMedia({ colorScheme: theme });
      await page.goto("/api");
      await expect(page.locator(".opblock").first()).toBeVisible({ timeout: 20000 });

      for (const [method, path] of operations) {
        await page.getByPlaceholder("Filter by tag or path").fill(path);
        await expect
          .poll(() =>
            page
              .locator(".opblock-summary-path")
              .evaluateAll(
                (elements, query) =>
                  elements.length > 0 &&
                  elements.every((element) => element.getAttribute("data-path")?.includes(query)),
                path
              )
          )
          .toBe(true);

        const operation = page
          .locator(`.opblock-${method}`)
          .filter({ has: page.locator(`[data-path="${path}"]`) })
          .first();

        await operation.locator(".opblock-summary-control").click();
        await expect(operation.locator(".opblock-body")).toBeVisible();
        await expect(operation.locator(".opblock-summary-operation-id")).toHaveCount(0);
        await expect(operation.getByText("No links", { exact: true }).first()).toBeHidden();
        await expect(operation.locator("select.content-type")).toHaveCount(0);
        await expect(operation.getByText("Controls Accept header.", { exact: true }).first()).toBeHidden();

        if (path === "/api/v1/sse/source-repositories/{id}/jobs/{job_id}/runs") {
          await expect(operation.locator('tr[data-param-in="path"]')).toHaveCount(2);

          for (const name of ["id", "job_id"]) {
            const row = operation.locator(`tr[data-param-name="${name}"]`);
            await expect(row.locator(".parameter__name.required")).toBeVisible();
            await expect(row.locator(".parameter__type")).toHaveText("string");
          }

          await expect(operation.locator(".response").first().locator(".vectis-media-type")).toHaveText(
            "text/event-stream"
          );
        }

        if (path === "/api/v1/source-schedules/{schedule_id}") {
          const row = operation.locator('tr[data-param-name="schedule_id"]');
          await expect(row.locator(".parameter__name.required")).toBeVisible();
          await expect(row.locator(".parameter__type")).toHaveText("string");
        }

        const layout = await operation.evaluate((element) => {
          const bounds = element.getBoundingClientRect();
          return [...element.querySelectorAll("table, .opblock-summary-control, .opblock-summary-path")]
            .filter((el) => el.getBoundingClientRect().width > 0)
            .map((el) => ({ name: el.className, fits: el.getBoundingClientRect().right <= bounds.right + 1 }));
        });

        expect(
          layout.filter((item) => !item.fits),
          path
        ).toEqual([]);

        expect(await page.evaluate(() => document.documentElement.scrollWidth), path).toBeLessThanOrEqual(width);

        for (const example of await operation.locator(".model-example").all()) {
          await example.getByRole("tab", { name: "Schema", exact: true }).click();
          const summary = example.locator(".vectis-schema > summary").first();
          if (await summary.count()) {
            await expect(summary).toBeVisible();
            await summary.click();
          }
        }

        if (path === "/api/v1/tokens") {
          const request = operation.locator(".opblock-section-request-body");
          await expect(request).toContainText("label");
          await expect(request).toContainText("expires_in");
          await operation.screenshot({ path: testInfo.outputPath("token-operation.png") });
        }

        const accessibility = await new AxeBuilder({ page })
          .include(".vectis-api-explorer")
          .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
          .analyze();

        expect(accessibility.violations, `${method} ${path}`).toEqual([]);
        await operation.locator(".opblock-summary-control").click();
      }

      await expect(page.getByRole("button", { name: "Try it out" })).toHaveCount(0);
      expect(errors).toEqual([]);

      const resources = page.getByRole("navigation", { name: "API reference resources" });
      for (const href of await resources
        .locator("a")
        .evaluateAll((links) => links.map((a) => a.getAttribute("href")))) {
        if (href?.startsWith("#")) {
          await expect(page.locator(href)).toHaveCount(1);
        } else if (href) {
          expect((await page.request.get(href)).ok(), href).toBe(true);
        }
      }
    });
  }
}

test("complete API inventory renders without errors", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/api");
  const operations = page.locator(".opblock-summary-control");
  await expect(operations.first()).toBeVisible({ timeout: 20000 });

  const count = await operations.count();
  expect(count).toBeGreaterThan(80);

  await operations.evaluateAll((buttons) => buttons.forEach((button) => (button as HTMLButtonElement).click()));
  await expect(page.locator(".opblock-body")).toHaveCount(count);

  const tabs = page.locator('.model-example button[data-name="model"]');
  await expect(tabs.first()).toBeVisible();

  await tabs.evaluateAll((buttons) => buttons.forEach((button) => (button as HTMLButtonElement).click()));
  const schemas = page.locator("section.models .model-container");
  const document = await (await page.request.get("/openapi/v1.json")).json();
  await expect(schemas).toHaveCount(Object.keys(document.components.schemas).length);

  await page.locator(".vectis-schema > summary").evaluateAll((summaries) =>
    summaries.forEach((summary) => {
      if (!summary.parentElement?.hasAttribute("open")) (summary as HTMLElement).click();
    })
  );

  await expect(page.getByText(/Could not render/)).toHaveCount(0);
  await expect(page.locator(".vectis-schema-table").first()).toBeVisible();
  expect(errors).toEqual([]);
});

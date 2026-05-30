import { expect, test } from "@playwright/test";

async function expectNonBlankPage() {
  const visibleText = document.body.innerText.trim();
  const bodyBox = document.body.getBoundingClientRect();

  return {
    bodyHeight: bodyBox.height,
    bodyWidth: bodyBox.width,
    textLength: visibleText.length
  };
}

test.describe("app smoke", () => {
  test("renders the cluster health page", async ({ page }) => {
    await page.goto("/health");

    await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();
    await expect(page.getByRole("navigation")).toContainText("Jobs");
    await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
    await expect(page.getByRole("table")).toContainText("Cell");

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
    expect(pageShape.bodyWidth).toBeGreaterThan(300);
    expect(pageShape.bodyHeight).toBeGreaterThan(300);
  });

  test("renders the jobs page with CRUD affordances", async ({ page }) => {
    await page.goto("/jobs");

    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(page.getByRole("button", { name: "New job" })).toBeVisible();
    await expect(page.getByRole("table")).toContainText("Job");
    await expect(page.getByLabel("Namespace")).toBeVisible();

    const pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
  });
});

test.describe("storybook smoke", () => {
  test("renders representative component and design stories", async ({ page }) => {
    await page.goto("http://127.0.0.1:6101/iframe.html?id=components-layout-appshell--dashboard&viewMode=story");

    await expect(page.getByRole("heading", { name: "Cluster dashboard" })).toBeVisible();
    await expect(page.getByRole("navigation")).toContainText("Health");

    let pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
    expect(pageShape.bodyWidth).toBeGreaterThan(300);

    await page.goto("http://127.0.0.1:6101/iframe.html?id=design-tokens--inventory&viewMode=story");

    await expect(page.getByRole("heading", { name: "Colors" })).toBeVisible();
    await expect(page.getByText("--surface-page", { exact: true })).toBeVisible();

    pageShape = await page.evaluate(expectNonBlankPage);
    expect(pageShape.textLength).toBeGreaterThan(100);
  });
});

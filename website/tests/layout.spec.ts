import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

declare global {
  interface Window {
    cspViolations: string[];
  }
}

for (const theme of ["light", "dark"] as const) {
  for (const route of ["/", "/docs/getting-started", "/docs/using/api-reference", "/api"]) {
    test(`production CSP layout ${theme} ${route}`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme: theme });
      await page.addInitScript((theme) => {
        localStorage.setItem("theme", theme);
        window.cspViolations = [];
        document.addEventListener("securitypolicyviolation", (event) => {
          window.cspViolations.push(`${event.violatedDirective}: ${event.blockedURI}`);
        });
      }, theme);

      await page.goto(route);
      await expect(page.locator("html")).toHaveAttribute("data-theme", theme);

      const toggle = page.getByRole("button", { name: /Switch between dark and light mode/ }).filter({ visible: true });
      await expect(toggle).toBeEnabled();
      expect((await page.locator(".navbar").boundingBox())!.y).toBe(0);

      const icons = await toggle
        .locator("svg")
        .evaluateAll((icons) => icons.filter((icon) => getComputedStyle(icon).display !== "none").length);

      expect(icons).toBe(1);

      if (route === "/api") await expect(page.locator(".swagger-ui .opblock").first()).toBeVisible({ timeout: 20000 });
      await expect(page.locator("h1").first()).toBeVisible();
      expect((await page.locator("h1").first().boundingBox())!.y).toBeLessThan(240);
      expect(await page.evaluate(() => window.cspViolations)).toEqual([]);

      await page.screenshot({ path: testInfo.outputPath("layout.png") });
      await toggle.click();
      if (theme === "dark") await toggle.click();

      const next = theme === "light" ? "dark" : "light";
      await expect(page.locator("html")).toHaveAttribute("data-theme", next);

      expect(await page.evaluate(() => localStorage.getItem("theme"))).toBe(next);
      expect(await page.evaluate(() => window.cspViolations)).toEqual([]);
    });
  }

  test(`mobile navigation and theme ${theme}`, async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.emulateMedia({ colorScheme: theme });
    await page.goto("/docs/getting-started");
    expect((await page.locator(".navbar").boundingBox())!.y).toBe(0);

    await page.getByRole("button", { name: "Toggle navigation bar" }).click();
    const toggle = page.getByRole("button", { name: /Switch between dark and light mode/ }).filter({ visible: true });
    await expect(toggle).toBeVisible();
    expect(
      await toggle
        .locator("svg")
        .evaluateAll((icons) => icons.filter((icon) => getComputedStyle(icon).display !== "none").length)
    ).toBe(1);

    await toggle.click();
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  });

  for (const width of [1280, 390]) {
    test(`API explorer operations and schema ${theme} ${width}px`, async ({ page }, testInfo) => {
      const foreign: string[] = [];
      page.on("request", (request) => {
        if (new URL(request.url()).hostname !== "127.0.0.1") foreign.push(request.url());
      });

      await page.setViewportSize({ width, height: 900 });
      await page.emulateMedia({ colorScheme: theme });
      await page.goto("/api");
      await expect(page.locator(".swagger-ui .opblock").first()).toBeVisible({ timeout: 20000 });

      const operation = page.locator(".opblock-get").filter({ has: page.locator('[data-path="/api/v1/runs"]') });
      await operation.locator(".opblock-summary-control").click();
      await expect(operation.getByText("Parameters", { exact: true })).toBeVisible();
      await expect(operation.getByText("Responses", { exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Try it out" })).toHaveCount(0);
      await expect(page.getByRole("link", { name: "Download OpenAPI JSON" })).toHaveAttribute(
        "href",
        "/openapi/v1.json"
      );

      await page.getByPlaceholder("Filter by tag or path").fill("/api/v1/runs");
      await expect(page.locator('[data-path="/health/live"]')).toHaveCount(0);
      await expect(page.locator('[data-path="/api/v1/runs"]').first()).toBeVisible();
      expect(foreign).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      const padding = await operation.locator("table:not(.model) th, table:not(.model) td").evaluateAll((cells) =>
        cells.map((cell) => ({
          left: parseFloat(getComputedStyle(cell).paddingLeft),
          right: parseFloat(getComputedStyle(cell).paddingRight)
        }))
      );

      expect(padding.length).toBeGreaterThan(0);
      expect(padding.every((cell) => cell.left >= 12 && cell.right >= 12)).toBe(true);

      const schema = page.locator('section.models .model-container[data-name="APIError"]');
      await schema.locator("summary").first().click();
      await expect(schema.locator("table.model").first()).toBeVisible();

      expect(
        await schema
          .locator("table.model td")
          .evaluateAll((cells) => cells.every((cell) => parseFloat(getComputedStyle(cell).paddingLeft) >= 12))
      ).toBe(true);

      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
      await expect(schema.locator("tbody tr")).toHaveCount(3);
      await expect(schema.locator("tbody summary")).toHaveCount(0);
      await expect(schema).toContainText("string");
      await expect(schema).toContainText("May be null.");
      await expect(schema).not.toContainText("[...]");

      const generic = page.locator('section.models .model-container[data-name="GenericObject"]');
      await expect(generic).toContainText("Object with arbitrary properties.");
      await expect(generic.locator("summary")).toHaveCount(0);

      const tableSurface = await operation
        .locator("table:not(.model)")
        .first()
        .evaluate((table) => {
          const header = table.querySelector("thead th, thead td");
          if (!header) throw new Error("Expected the operation table to have a header.");
          return {
            table: getComputedStyle(table).backgroundColor,
            header: getComputedStyle(header).backgroundColor
          };
        });

      expect(tableSurface.table).not.toBe("rgba(0, 0, 0, 0)");
      expect(tableSurface.table).not.toBe(tableSurface.header);

      const token = page.locator('section.models .model-container[data-name="CreateTokenRequest"]');
      await token.locator(":scope > details > summary").click();
      await expect(token).toContainText('Allowed values: "1w"');
      const scopes = token
        .locator("tbody tr")
        .filter({ has: page.getByRole("rowheader", { name: "scopes", exact: true }) });

      await scopes.locator("summary").first().focus();
      await page.keyboard.press("Enter");
      await scopes.getByText("Items", { exact: true }).click();
      await expect(scopes.getByRole("rowheader", { name: "namespace_path", exact: true })).toBeVisible();

      const binding = page.locator('section.models .model-container[data-name="BindingResponse"]');
      await binding.locator("summary").first().click();
      await expect(binding).toContainText('Allowed values: "viewer", "trigger", "operator", "admin"');

      const composite = page.locator('section.models .model-container[data-name="JobDefinitionRequest"]');
      await composite.locator("summary").first().click();
      await expect(composite).toContainText("Exactly one of:");
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

      await schema.screenshot({ path: testInfo.outputPath("schema.png") });
      const accessibility = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
      expect(accessibility.violations).toEqual([]);

      await page.screenshot({ path: testInfo.outputPath("explorer.png") });
    });
  }
}

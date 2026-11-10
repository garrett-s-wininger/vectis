import { openNavigation, closeNavigation, navigatePrimary, expectNamespace } from "./navigationHelpers";
import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 820, 1280]) {
    test(`repository workflow ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.route("**/ui/api/context", (route) =>
        route.fulfill({ json: { auth_enabled: true, principal: { kind: "local_user", username: "admin" } } })
      );

      await page.goto("/repositories");
      await expect(page.getByRole("button", { name: "Inspect vectis-local", exact: true })).toBeVisible();
      await page.screenshot({ path: testInfo.outputPath("repositories.png") });
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await openNavigation(page);
      const primary = page.getByRole("navigation", { name: "Primary", exact: true });
      for (const link of await primary.getByRole("link").all()) {
        const box = (await link.boundingBox())!;
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(width);
      }

      await closeNavigation(page);
      await page.getByRole("button", { name: "Inspect vectis-local", exact: true }).click();
      await expect(page.getByRole("heading", { name: "vectis-local", exact: true })).toBeVisible();
      await expect(page.getByText("Available", { exact: true })).toBeVisible();
      await expect(page.getByText("No valid job definitions discovered.", { exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Check checkout", exact: true }).click();
      await expect(page.getByRole("status").filter({ hasText: "Checkout check completed." })).toBeVisible();
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      const overflow = await page.evaluate(() => ({
        body: document.documentElement.scrollWidth > innerWidth,
        main: document.querySelector("main")!.scrollWidth > document.querySelector("main")!.clientWidth
      }));

      expect(overflow).toEqual({ body: false, main: false });

      await page.screenshot({ path: testInfo.outputPath("repository-detail.png"), fullPage: true });
    });
  }
}

test("diagnostics explain missing checkouts without presenting an empty source", async ({ page }) => {
  await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-repositories--failed-discovery&viewMode=story");
  await expect(page.getByRole("heading", { name: "vectis-local", exact: true })).toBeVisible({ timeout: 20000 });
  await expect(page.getByText("The configured checkout is not available.")).toBeVisible();
  await expect(page.getByText("Cannot scan definitions: checkout is missing.")).toBeVisible();
  await expect(page.getByText("No valid job definitions discovered.")).toHaveCount(0);
});

test("shows invalid definitions and keeps read-only users from syncing", async ({ page }) => {
  await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-repositories--invalid-definitions&viewMode=story");
  await expect(page.getByRole("heading", { name: "vectis-local", exact: true })).toBeVisible({ timeout: 20000 });
  await expect(page.getByRole("button", { name: "View build", exact: true })).toBeVisible();
  await expect(page.getByText("actions[0]: unknown action type", { exact: true })).toBeVisible();

  await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-repositories--viewer&viewMode=story");
  await expect(page.getByText("Available", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sync now", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Check checkout", exact: true })).toHaveCount(0);
});

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280]) {
    test(`repository discovery presentation ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-repositories--invalid-definitions&viewMode=story");
      await expect(page.getByRole("button", { name: "View build", exact: true })).toBeVisible({ timeout: 20000 });

      const jobs = page.getByRole("heading", { name: "Discovered jobs", exact: true });
      const source = page.getByRole("heading", { name: "Source", exact: true });
      expect((await source.boundingBox())!.y).toBeLessThan((await jobs.boundingBox())!.y);

      await expect(page.getByRole("heading", { name: "Synchronization", exact: true })).toHaveCount(0);
      await expect(page.getByRole("heading", { name: "Checkout health", exact: true })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Copy Revision", exact: true })).toHaveCount(1);
      expect(
        await page
          .locator("table")
          .first()
          .evaluate((table) => getComputedStyle(table.parentElement!, "::before").content)
      ).toBe("none");

      expect(
        await page
          .locator("table td")
          .first()
          .evaluate((cell) => getComputedStyle(cell, "::after").content)
      ).toBe("none");

      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      expect(
        await page.evaluate(
          () => document.querySelector("main")!.scrollWidth <= document.querySelector("main")!.clientWidth
        )
      ).toBe(true);

      await page.screenshot({ path: testInfo.outputPath("discovery.png"), fullPage: true });
    });
  }
}

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 820, 1280]) {
    test(`shared connection access ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("/manage/repositories");
      await expect(page.getByRole("heading", { name: "Repository connections", exact: true })).toBeVisible();
      await expect(page.getByText("Instance administration", { exact: true })).toHaveCount(0);
      await expect(page.getByRole("banner").getByTitle("Instance", { exact: true })).toBeVisible();

      await page.screenshot({ path: testInfo.outputPath("connections.png"), fullPage: true });
      await closeNavigation(page);
      await page.getByRole("button", { name: "Inspect vectis-local", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Namespace access", exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Discovered jobs", exact: true })).toHaveCount(0);

      await page.getByRole("button", { name: "Grant access", exact: true }).click();
      await expect(page.getByRole("button", { name: "Revoke access", exact: true })).toHaveCount(2);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      expect(
        await page.evaluate(() => ({
          body: document.documentElement.scrollWidth > innerWidth,
          main: document.querySelector("main")!.scrollWidth > document.querySelector("main")!.clientWidth
        }))
      ).toEqual({ body: false, main: false });

      await page.screenshot({ path: testInfo.outputPath("namespace-access.png"), fullPage: true });
      await navigatePrimary(page, "Repositories");
      await openNavigation(page);
      await page.getByLabel("Namespace: Root", { exact: true }).click();
      await page.getByRole("treeitem", { name: "/team-a", exact: true }).click();
      await expect(page.getByRole("button", { name: "Inspect vectis-local", exact: true })).toBeVisible();

      await expectNamespace(page, "/team-a");
      await navigatePrimary(page, "Jobs");
      await expect(page.getByText("api-test-suite", { exact: true })).toBeVisible();
      await expectNamespace(page, "/team-a");

      await page.goBack();
      await expect(page.getByRole("heading", { name: "Repositories", exact: true })).toBeVisible();
      await expectNamespace(page, "/team-a");
    });
  }
}

for (const colorScheme of ["light", "dark"] as const) {
  test(`compact workspace and page hierarchy ${colorScheme}`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme });
    await page.setViewportSize({ width: 390, height: 900 });
    await page.route("**/ui/api/context", (route) =>
      route.fulfill({ json: { auth_enabled: true, principal: { username: "admin" } } })
    );

    await page.goto("/jobs?namespace=%2Fteam-a");
    await expect(page.getByRole("heading", { name: "Jobs", exact: true })).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Jobs location", exact: true })).toHaveCount(0);
    expect(
      await page.getByRole("main").evaluate((main) => main.previousElementSibling!.getBoundingClientRect().height)
    ).toBeLessThanOrEqual(88);

    await openNavigation(page);
    await page.getByRole("button", { name: "Namespace: /team-a", exact: true }).click();
    await expect(page.getByLabel("Effective access: Admin", { exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "View namespace", exact: true })).toHaveAttribute(
      "href",
      "/namespaces?namespace=%2Fteam-a"
    );

    expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
      []
    );

    await page.screenshot({ path: testInfo.outputPath("compact-workspace.png") });
    await page.getByRole("link", { name: "View namespace", exact: true }).click();
    await expect(page.getByRole("heading", { name: "/team-a", exact: true })).toBeVisible();

    await page.reload();
    await expect(page.getByRole("heading", { name: "/team-a", exact: true })).toBeVisible();

    await page.goBack();
    await expect(page.getByRole("heading", { name: "Jobs", exact: true })).toBeVisible();

    await page.getByRole("button", { name: "View api-test-suite", exact: true }).click();
    await expect(page.getByRole("navigation", { name: "Job location", exact: true })).toBeVisible();

    for (const path of ["/manage/repositories", "/profile"]) {
      await page.goto(path);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
      await expect(
        page.getByRole("banner").getByTitle(path === "/profile" ? "My account" : "Instance", { exact: true })
      ).toBeVisible();

      expect(await page.getByRole("main").evaluate((main) => main.previousElementSibling!.tagName)).toBe("HEADER");
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.screenshot({ path: testInfo.outputPath(path === "/profile" ? "profile.png" : "administration.png") });
    }
  });
}

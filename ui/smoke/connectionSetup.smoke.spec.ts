import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { visibleTextContrast } from "./contrast";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280]) {
    test(`public connection setup and editing ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 950 });
      await page.goto("/manage/repositories");
      await page.getByRole("button", { name: "Add connection", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Add connection", exact: true })).toBeVisible();
      await expect(page.getByLabel("Repository ID", { exact: true })).toBeVisible();

      await page.getByLabel("Repository ID", { exact: true }).fill("vectis");
      await page.getByLabel("Repository URL", { exact: true }).fill("https://github.com/garrett-s-wininger/vectis.git");
      await expect(page.getByLabel("Repository URL", { exact: true })).toHaveValue(
        "https://github.com/garrett-s-wininger/vectis.git"
      );

      await expect(page.getByLabel("Credential", { exact: true })).toBeEnabled();
      await expect(page.getByLabel(/^Credential:/)).toHaveText("None — Public access");

      const validationPanel = page.getByRole("region", { name: "Connection validation", exact: true });
      const initialPanelHeight = (await validationPanel.boundingBox())!.height;
      const validationSpacing = await page
        .getByRole("group", { name: "Validation details", exact: true })
        .evaluate((element) => {
          const box = element.getBoundingClientRect();
          return {
            top: element.firstElementChild!.getBoundingClientRect().top - box.top,
            bottom: box.bottom - element.lastElementChild!.getBoundingClientRect().bottom
          };
        });

      expect(validationSpacing.top).toBeCloseTo(validationSpacing.bottom, 0);

      const save = page.getByRole("button", { name: "Save", exact: true });
      const initialSavePosition = await save.evaluate((element) => (element as HTMLElement).offsetTop);
      const initialSaveWidth = (await save.boundingBox())!.width;
      const connectionEnabled = page.getByRole("checkbox", { name: "Connection enabled", exact: true });
      await connectionEnabled.focus();
      await connectionEnabled.press("Space");
      expect((await save.boundingBox())!.width).toBeCloseTo(initialSaveWidth, 0);

      await connectionEnabled.press("Space");
      expect((await save.boundingBox())!.width).toBeCloseTo(initialSaveWidth, 0);

      await page.getByLabel(/^Credential:/).click();
      const search = page.getByRole("searchbox", { name: "Search credential", exact: true });
      await expect(search).toBeFocused();

      await search.fill("missing-credential");
      await expect(
        page.getByRole("listbox").getByRole("option", { name: "No matching options.", exact: true })
      ).toBeVisible();

      await search.fill("encryptedfs");
      const choice = page.getByRole("listbox").getByRole("option", { name: /git\/example/ });
      await expect(choice).toContainText("Encrypted filesystem");
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.screenshot({ path: testInfo.outputPath("credential-search.png") });
      await search.press("ArrowDown");
      await expect(choice).toBeFocused();

      await choice.press("Enter");
      await expect(page.getByLabel(/^Credential:/)).toHaveText("git/example");
      expect(await save.evaluate((element) => (element as HTMLElement).offsetTop)).toBe(initialSavePosition);

      await page.getByLabel(/^Credential:/).click();
      await search.fill("public access");
      await search.press("Enter");
      await expect(page.getByLabel(/^Credential:/)).toHaveText("None — Public access");
      await expect(page.getByRole("heading", { name: "Connection saved", exact: true })).toHaveCount(0);

      if (width > 960) {
        const url = await page.getByLabel("Repository URL", { exact: true }).boundingBox();
        const repositoryID = await page.getByLabel("Repository ID", { exact: true }).boundingBox();
        const ref = await page.getByLabel("Default ref", { exact: true }).boundingBox();
        const credential = await page.getByLabel(/^Credential:/).boundingBox();
        const namespace = await page.getByLabel(/^Initial namespace access:/).boundingBox();
        expect(url!.width).toBeGreaterThan(repositoryID!.width * 1.9);
        expect(Math.abs(repositoryID!.y - ref!.y)).toBeLessThan(5);
        expect(Math.abs(credential!.y - namespace!.y)).toBeLessThan(5);
      }

      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.screenshot({ path: testInfo.outputPath("connection-form.png"), fullPage: true });
      await page.getByRole("button", { name: "Validate connection", exact: true }).click();
      await expect(page.getByText("Connection valid", { exact: true })).toBeVisible();
      expect((await validationPanel.boundingBox())!.height).toBeCloseTo(initialPanelHeight, 0);
      expect(await save.evaluate((element) => (element as HTMLElement).offsetTop)).toBe(initialSavePosition);
      await expect(page.getByRole("button", { name: "Revalidate", exact: true })).toBeVisible();
      await expect(page.getByText("b28a74c41946e6952910ddab7e5bd791d5bd0194", { exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Connection saved", exact: true })).toHaveCount(0);

      await page.screenshot({ path: testInfo.outputPath("connection-preview.png"), fullPage: true });
      await page.getByRole("button", { name: "Save", exact: true }).click();
      await expect(page.getByText("Validated", { exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Connection saved", exact: true })).toBeFocused();
      await expect(page.getByText("1 job definition discovered.", { exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "View jobs", exact: true })).toBeInViewport();

      await page.screenshot({ path: testInfo.outputPath("connection-validated.png") });
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.getByRole("button", { name: "Manage connection", exact: true }).click();
      await expect(page.getByRole("heading", { name: "vectis", exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Namespace access", exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Edit connection", exact: true }).click();
      await expect(page.getByLabel("Repository ID", { exact: true })).toHaveValue("vectis");
      await expect(page.getByLabel("Repository ID", { exact: true })).toHaveAttribute("readonly");

      await page.getByLabel("Default ref", { exact: true }).fill("release/demo");
      await page.getByRole("button", { name: "Save", exact: true }).click();
      await expect(page.getByText("Validated", { exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Edit settings", exact: true }).click();
      await expect(page.getByLabel("Default ref", { exact: true })).toBeEnabled();
      await expect(page.getByLabel("Default ref", { exact: true })).toHaveValue("release/demo");

      await page.getByRole("button", { name: "Save", exact: true }).click();
      await expect(page.getByText("Validated", { exact: true })).toBeVisible();

      await page.getByRole("button", { name: "View jobs", exact: true }).click();
      await expect(page.getByRole("heading", { name: "vectis", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "View ci", exact: true })).toBeVisible();
      await expect(page.getByText(".vectis.json", { exact: true })).toBeVisible();

      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      await page.screenshot({ path: testInfo.outputPath("connected-repository.png") });

      if (width <= 900) await page.getByRole("button", { name: "Open navigation", exact: true }).click();
      await page.getByRole("link", { name: "Repository connections", exact: true }).click();
      await page.getByRole("button", { name: "Inspect vectis", exact: true }).click();
      await page.getByRole("button", { name: "Edit connection", exact: true }).click();
      const enabled = page.getByRole("checkbox", { name: "Connection enabled", exact: true });
      await enabled.focus();
      await enabled.press("Space");
      await expect(enabled).not.toBeChecked();
      await expect(page.getByRole("heading", { name: "Review affected jobs", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Save", exact: true })).toBeDisabled();

      const reviewed = page.getByRole("checkbox", { name: "I reviewed the affected jobs and automation", exact: true });
      await reviewed.focus();
      await reviewed.press("Space");
      await expect(reviewed).toBeChecked();

      await page.getByRole("button", { name: "Save", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Connection saved", exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Manage connection", exact: true }).click();
      await page.getByRole("button", { name: "Delete connection", exact: true }).click();
      await expect(page.getByRole("button", { name: "Confirm deletion", exact: true })).toBeVisible();
      expect(await page.locator("main").evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
      expect((await visibleTextContrast(page)).failures).toEqual([]);

      await page.screenshot({ path: testInfo.outputPath("delete-connection.png") });
      await page.getByRole("button", { name: "Confirm deletion", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Repository connections", exact: true })).toBeVisible();
      await expect(page.getByRole("button", { name: "Inspect vectis", exact: true })).toHaveCount(0);
    });
  }
}

test("configuration-owned connection settings remain read-only on a direct editor URL", async ({ page }) => {
  await page.goto("/manage/repositories/vectis-local?editor=edit");
  await expect(page.getByRole("heading", { name: "Managed by configuration", exact: true })).toBeVisible();
  await expect(page.getByLabel("Repository URL", { exact: true })).toHaveCount(0);

  await page.getByRole("button", { name: "View connection", exact: true }).click();
  await expect(page.getByRole("heading", { name: "vectis-local", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Edit connection", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Delete connection", exact: true })).toHaveCount(0);
});

test("validation errors stay on the editable form before a connection is saved", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 950 });
  await page.goto(
    "http://127.0.0.1:6101/iframe.html?id=pages-repositories--connection-validation-failure&viewMode=story"
  );

  await expect(page.getByRole("heading", { name: "Add connection", exact: true })).toBeVisible({ timeout: 20000 });

  await page.getByLabel("Repository ID", { exact: true }).fill("vectis");
  await page.getByLabel("Repository URL", { exact: true }).fill("https://github.com/garrett-s-wininger/vectis.git");
  await page.getByRole("button", { name: "Validate connection", exact: true }).click();
  await expect(page.getByRole("region", { name: "Connection validation", exact: true })).toBeFocused();
  await expect(
    page.getByText("The remote could not be reached. Check the URL or retry later.", { exact: true })
  ).toBeInViewport();

  await expect(page.getByLabel("Repository URL", { exact: true })).toBeEnabled();
  await expect(page.getByRole("heading", { name: "Connection saved", exact: true })).toHaveCount(0);

  await page.getByRole("button", { name: "Validate connection", exact: true }).click();
  await expect(
    page.getByText("The remote could not be reached. Check the URL or retry later.", { exact: true })
  ).toBeVisible();

  await expect(page.getByRole("button", { name: "Validate connection", exact: true })).toBeEnabled();
  expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual([]);

  await page.screenshot({ path: testInfo.outputPath("connection-validation-error.png") });
});

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { visibleTextContrast } from "./contrast";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280, 1440]) {
    test(`job workspace with active hotfix ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1100 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-jobs--hotfix-active&viewMode=story");
      await expect(page.getByRole("button", { name: "Edit", exact: true })).toBeVisible({ timeout: 20000 });
      await expect(page.getByRole("button", { name: /Edit source for/ })).toHaveCount(0);

      const compare = page.getByRole("checkbox", { name: "Definition comparison", exact: true });
      await expect(compare).not.toBeChecked();
      await expect(page.getByLabel("Effective definition", { exact: true })).toContainText("exit 0");
      await expect(page.getByLabel("Effective definition", { exact: true })).not.toContainText("exit 7");

      await page.screenshot({ path: testInfo.outputPath("job-default-effective.png"), fullPage: true });
      await compare.focus();
      await compare.press("Space");
      await expect(compare).toBeChecked();
      await expect(page.getByRole("heading", { name: "Active hotfix", exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Definition contents", exact: true })).toHaveCount(0);

      const definitionHeader = page.locator('section[aria-labelledby="job-definition-title"] > div').first();
      if (width >= 1280) expect((await definitionHeader.boundingBox())!.height).toBeLessThanOrEqual(42);
      const titleBox = (await page.getByRole("heading", { name: "Definition", exact: true }).boundingBox())!;
      const toggleBox = (await page
        .getByRole("checkbox", { name: "Definition comparison", exact: true })
        .locator("..")
        .boundingBox())!;

      if (width === 1280)
        expect(Math.abs(titleBox.y + titleBox.height / 2 - (toggleBox.y + toggleBox.height / 2))).toBeLessThan(2);

      const metadata = page.locator('dl[aria-label="Repository source"]');
      await expect(metadata).toContainText("Namespace");

      const activity = page.getByRole("complementary", { name: "Job context", exact: true });
      await expect(activity.getByRole("list", { name: "Recent runs", exact: true }).locator("li")).toHaveCount(5);
      await expect(activity.getByRole("button", { name: "View all runs for api-test-suite", exact: true })).toHaveText(
        /All/
      );

      await expect(
        activity.getByRole("list", { name: "Recent runs", exact: true }).getByRole("button").first()
      ).toContainText("#1240");

      await expect(activity.getByRole("list", { name: "Recent runs", exact: true })).not.toContainText("Run #");

      const rowAlignment = await activity
        .getByRole("list", { name: "Recent runs", exact: true })
        .locator("li button")
        .evaluateAll((buttons) =>
          buttons.map((button) => ({
            left: button.firstElementChild!.firstElementChild!.getBoundingClientRect().left,
            right: button.lastElementChild!.lastElementChild!.getBoundingClientRect().right
          }))
        );

      expect(
        Math.max(...rowAlignment.map((row) => row.left)) - Math.min(...rowAlignment.map((row) => row.left))
      ).toBeLessThanOrEqual(1);

      expect(
        Math.max(...rowAlignment.map((row) => row.right)) - Math.min(...rowAlignment.map((row) => row.right))
      ).toBeLessThanOrEqual(1);

      const activityBox = (await activity.boundingBox())!;
      const definitionBox = (await page.getByRole("region", { name: "Definition", exact: true }).boundingBox())!;
      const pageBox = (await page.getByRole("region", { name: "api-test-suite", exact: true }).boundingBox())!;
      const headerBox = (await page.locator('section[aria-labelledby="job-detail-title"] > header').boundingBox())!;
      expect(headerBox.width).toBeCloseTo(pageBox.width, 0);

      if (width >= 1280) {
        expect(activityBox.x).toBeGreaterThan(definitionBox.x + definitionBox.width);
        expect(activityBox.y).toBeCloseTo(definitionBox.y, 0);
      } else {
        expect(activityBox.y).toBeGreaterThan(definitionBox.y + definitionBox.height);
      }

      const provenance = page.getByRole("region", { name: "Provenance", exact: true });
      const history = page.getByRole("region", { name: "Recent runs", exact: true });
      const triggers = page.getByRole("region", { name: "Triggers", exact: true });
      const provenanceBox = (await provenance.boundingBox())!;
      const historyBox = (await history.boundingBox())!;
      const triggersBox = (await triggers.boundingBox())!;
      expect(provenanceBox.y + provenanceBox.height).toBeLessThan(historyBox.y);
      expect(historyBox.y + historyBox.height).toBeLessThan(triggersBox.y);

      const facts = await metadata.locator(":scope > div").all();
      const factBoxes = await Promise.all(facts.map((fact) => fact.boundingBox()));
      for (let index = 0; index < factBoxes.length; index++) {
        expect(factBoxes[index]!.width).toBeCloseTo(factBoxes[0]!.width, 0);

        if (index > 0)
          expect(factBoxes[index]!.y).toBeGreaterThan(factBoxes[index - 1]!.y + factBoxes[index - 1]!.height);
      }

      const diff = page.getByLabel("Source to Effective diff", { exact: true });
      await expect(diff).toContainText("exit 7");
      await expect(diff).toContainText("exit 0");

      const metadataBox = (await metadata.locator("..").boundingBox())!;
      const diffBox = (await diff.boundingBox())!;
      const frame = page.getByLabel("Definition frame", { exact: true });
      const frameBox = (await frame.boundingBox())!;
      expect(Math.abs(diffBox.width - frameBox.width)).toBeLessThanOrEqual(2);
      expect(metadataBox.width).toBeLessThan(activityBox.width);

      if (width >= 1280) expect(metadataBox.x).toBeGreaterThan(frameBox.x + frameBox.width);
      else expect(metadataBox.y).toBeGreaterThan(frameBox.y + frameBox.height);

      const hotfixBox = (await page.getByRole("region", { name: "Active hotfix", exact: true }).boundingBox())!;
      expect(hotfixBox.y + hotfixBox.height).toBeLessThan(diffBox.y);
      await expect(page.getByRole("region", { name: "Provenance", exact: true })).toContainText("Commit");
      await expect(
        page
          .getByRole("region", { name: "Active hotfix", exact: true })
          .getByRole("button", { name: "Edit", exact: true })
      ).toBeVisible();

      await expect(page.getByRole("complementary", { name: "Job settings", exact: true })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Revert", exact: true })).toBeVisible();

      const beforeHeader = await definitionHeader.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight,
        width: element.clientWidth
      }));

      const beforeEdit = await frame.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight
      }));

      await page.getByRole("button", { name: "Edit", exact: true }).click();
      await expect(page.getByLabel("Reason for changes", { exact: true })).toBeFocused();

      const duringEdit = await frame.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight
      }));

      expect(duringEdit.height).toBe(beforeEdit.height);
      expect(duringEdit.top).toBe(beforeEdit.top);

      const duringHeader = await definitionHeader.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight,
        width: element.clientWidth
      }));

      expect(duringHeader).toEqual(beforeHeader);

      const editor = page.getByRole("textbox", { name: "Definition", exact: true });
      const editorBox = (await editor.boundingBox())!;
      expect(Math.abs(editorBox.width - frameBox.width)).toBeLessThanOrEqual(2);

      await page.getByRole("button", { name: "Cancel", exact: true }).click();
      await page.getByRole("heading", { name: "api-test-suite", exact: true }).scrollIntoViewIfNeeded();
      await page.screenshot({ path: testInfo.outputPath("job-workspace-overview.png"), fullPage: true });
      await diff.scrollIntoViewIfNeeded();
      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      expect(await page.locator("main").evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
      await page.screenshot({ path: testInfo.outputPath("job-workspace-definition.png"), fullPage: true });
    });
  }
}

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { visibleTextContrast } from "./contrast";

const fixed = JSON.stringify(
  { root: { id: "root", uses: "builtins/script", with: { script: "printf 'Repaired demo\\n'\nsleep 5\nexit 0" } } },
  null,
  2
);

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [390, 1280]) {
    test(`owned source hotfix review and restoration ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 1000 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-source-job-definition--hotfix-demo&viewMode=story");
      const assign = page.getByRole("button", { name: "Assign owner", exact: true });
      await expect(assign).toBeVisible({ timeout: 20000 });

      const owner = await page.locator("summary").filter({ hasText: "/team-a" }).boundingBox();
      const action = await assign.boundingBox();
      expect(action!.y).toBeGreaterThan(owner!.y + owner!.height);
      expect(action!.width).toBeCloseTo(owner!.width, 0);

      const preview = page.getByRole("region", { name: "Definition", exact: true }).locator("pre");
      const compare = page.getByRole("checkbox", { name: "Definition comparison", exact: true });
      await expect(compare).toBeDisabled();
      await expect(compare).not.toBeChecked();

      await page.screenshot({ path: testInfo.outputPath("definition-unassigned.png") });
      await page.getByRole("button", { name: "Assign owner", exact: true }).click({ timeout: 20000 });
      await expect(page.getByText("Job owner assigned.", { exact: true })).toBeVisible();

      const edit = async () => {
        await page.getByRole("button", { name: /^(Hotfix|Edit)$/ }).click();
        await expect(page.getByLabel("Reason for changes", { exact: true })).toBeFocused();

        await page.getByRole("textbox", { name: "Definition", exact: true }).fill(fixed);
        await page.getByLabel("Reason for changes", { exact: true }).fill("Restore production execution");
        const reasonBox = (await page.getByLabel("Reason for changes", { exact: true }).boundingBox())!;
        const editorBox = (await page.getByRole("textbox", { name: "Definition", exact: true }).boundingBox())!;
        expect(reasonBox.y + reasonBox.height).toBeLessThan(editorBox.y);
        await expect(page.getByRole("button", { name: "Review", exact: true })).toHaveCount(1);
      };

      await edit();
      await page.getByRole("textbox", { name: "Definition", exact: true }).fill("{invalid");
      await page.getByRole("button", { name: "Review", exact: true }).click();
      const validation = page.getByRole("alert");
      await expect(validation).toContainText("Check your changes");
      await expect(page.getByRole("button", { name: "Reload definition", exact: true })).toHaveCount(0);

      await validation.scrollIntoViewIfNeeded();
      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.screenshot({ path: testInfo.outputPath("hotfix-invalid-changes.png") });
      await page.getByRole("button", { name: "Reset", exact: true }).click();
      await expect(page.getByRole("alert")).toHaveCount(0);
      await expect(page.getByLabel("Reason for changes", { exact: true })).toHaveValue("");
      await expect(page.getByRole("textbox", { name: "Definition", exact: true })).toHaveValue(/exit 7/);

      await page.getByRole("textbox", { name: "Definition", exact: true }).fill(fixed);
      await page.getByLabel("Reason for changes", { exact: true }).fill("Restore production execution");
      await page.getByRole("button", { name: "Review", exact: true }).click();
      await expect(page.getByLabel("Effective to Proposed diff", { exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Run once", exact: true }).click();
      await expect(page.getByText("One-off run submitted.", { exact: true })).toBeVisible();
      await expect(page.getByRole("heading", { name: "Active hotfix", exact: true })).toHaveCount(0);
      await edit();

      await page.getByRole("button", { name: "Review", exact: true }).click();
      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect((await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze()).violations).toEqual(
        []
      );

      await page.getByRole("heading", { name: "Review changes", exact: true }).scrollIntoViewIfNeeded();
      await page.screenshot({ path: testInfo.outputPath("hotfix-review.png") });
      await page.getByRole("button", { name: "Activate", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Active hotfix", exact: true })).toBeVisible();
      await expect(compare).toBeEnabled();
      await expect(compare).not.toBeChecked();

      await compare.focus();
      await compare.press("Space");
      await expect(compare).toBeChecked();

      const diff = page.getByLabel("Source to Effective diff", { exact: true });
      await expect(diff).toContainText("exit 7");
      await expect(diff).toContainText("exit 0");
      await expect(page.getByRole("button", { name: "Copy effective JSON", exact: true })).toBeVisible();

      const frame = page.getByLabel("Definition frame", { exact: true });
      const comparedFrame = (await frame.boundingBox())!;
      await compare.focus();
      await compare.press("Space");
      await expect(preview).toContainText("exit 0");
      await expect(preview).not.toContainText("exit 7");
      await expect(compare.locator("..")).toContainText("Effective");

      const jsonFrame = (await frame.boundingBox())!;
      expect(jsonFrame.height).toBe(comparedFrame.height);
      expect(jsonFrame.y).toBe(comparedFrame.y);

      await page.getByRole("heading", { name: "Active hotfix", exact: true }).scrollIntoViewIfNeeded();
      expect((await visibleTextContrast(page)).failures).toEqual([]);
      expect(await page.locator("main").evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);

      await page.screenshot({ path: testInfo.outputPath("hotfix-active.png") });
      const beforeRevert = await frame.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight
      }));

      await page.getByRole("button", { name: "Revert", exact: true }).click();
      await expect(page.getByLabel("Effective to Source diff", { exact: true })).toBeVisible();

      const revertingFrame = await frame.evaluate((element) => ({
        top: (element as HTMLElement).offsetTop,
        height: element.clientHeight
      }));

      expect(revertingFrame.height).toBe(beforeRevert.height);
      expect(revertingFrame.top).toBe(beforeRevert.top);

      const restoration = page.getByRole("form", { name: "Revert hotfix", exact: true });
      await restoration.getByRole("button", { name: "Revert", exact: true }).click();
      await expect(page.getByRole("heading", { name: "Active hotfix", exact: true })).toBeVisible();

      await restoration.getByRole("button", { name: "Confirm revert", exact: true }).click();
      await expect(
        page.getByText("Hotfix reverted. Future runs use the source definition.", { exact: true })
      ).toBeVisible();

      await expect(page.getByRole("region", { name: "Definition", exact: true }).locator("pre")).toContainText(
        "exit 7"
      );

      await expect(compare).toBeDisabled();
      await expect(compare).not.toBeChecked();
    });
  }
}

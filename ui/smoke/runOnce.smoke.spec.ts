import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { visibleTextContrast } from "./contrast";

const examplePath = fileURLToPath(new URL("../../examples/log-viewer.json", import.meta.url));

for (const colorScheme of ["light", "dark"] as const) {
  test(`Run Once file import and editor consistency ${colorScheme}`, async ({ page }, testInfo) => {
    const definition = await readFile(examplePath, "utf8");
    await page.emulateMedia({ colorScheme });
    await page.addInitScript(() => {
      const read = FileReader.prototype.readAsText;
      FileReader.prototype.readAsText = function (file, encoding) {
        setTimeout(() => read.call(this, file, encoding), 800);
      };
    });

    await page.goto("/runs");
    await page.getByRole("button", { name: "Run Once", exact: true }).click();

    const editor = page.getByLabel("Definition JSON", { exact: true });
    const appearance = () =>
      editor.evaluate((element) => {
        const style = getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return {
          background: style.background,
          color: style.color,
          border: style.border,
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height
        };
      });

    await page.evaluate(() => document.fonts.ready);

    const loadButton = page.getByRole("button", { name: "Load JSON file", exact: true });
    const submitButton = page.getByRole("button", { name: "Submit run", exact: true });
    await loadButton.hover();

    const probe = await editor.evaluateHandle((editor) => {
      const panel = editor.closest("section")!;
      const buttons = [...panel.querySelectorAll("button")];
      const buttonAppearance = () =>
        buttons.map((button) => {
          const style = getComputedStyle(button);
          const rect = button.getBoundingClientRect();
          return {
            background: style.background,
            color: style.color,
            opacity: style.opacity,
            shadow: style.boxShadow,
            transform: style.transform,
            x: rect.x,
            y: rect.y,
            width: rect.width,
            height: rect.height
          };
        });

      const state = {
        baseline: buttonAppearance(),
        samples: [] as ReturnType<typeof buttonAppearance>[],
        outsideMutations: [] as string[],
        removedButtons: false
      };

      const observer = new MutationObserver((records) => {
        for (const record of records) {
          if (!panel.contains(record.target)) {
            const target = record.target as Element;
            state.outsideMutations.push(
              `${record.type}:${target.tagName}:${target.getAttribute("aria-label") ?? target.textContent?.slice(0, 40)}:${record.attributeName}`
            );
          }
        }

        if (buttons.some((button) => !button.isConnected)) state.removedButtons = true;
      });

      observer.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true });

      let active = true;
      function sample() {
        if (!active) return;
        if ((editor as HTMLTextAreaElement).readOnly) state.samples.push(buttonAppearance());
        requestAnimationFrame(sample);
      }

      requestAnimationFrame(sample);

      return {
        state,
        stop: () => {
          active = false;
          observer.disconnect();
        }
      };
    });

    const before = await appearance();
    await editor.evaluate((element) => {
      element.dataset.importIdentity = "original";
    });

    const chooserEvent = page.waitForEvent("filechooser");
    await loadButton.click();
    await (await chooserEvent).setFiles(examplePath);
    await expect(editor).toHaveAttribute("readonly");
    await expect(editor).toBeEnabled();
    expect(await appearance()).toEqual(before);

    await expect(page.getByRole("button", { name: "Load JSON file", exact: true })).toBeEnabled();
    await expect(submitButton).toHaveAttribute("aria-disabled", "true");
    await expect(submitButton).not.toHaveAttribute("disabled");

    await submitButton.dispatchEvent("click");
    await expect(page).toHaveURL(/\/runs$/);
    await expect(editor).toHaveValue(definition);
    await expect(editor).not.toHaveAttribute("readonly");
    await expect(editor).toHaveAttribute("data-import-identity", "original");
    expect(await appearance()).toEqual(before);

    const observed = await probe.evaluate((probe) => {
      probe.stop();
      return probe.state;
    });

    await probe.dispose();
    expect(observed.samples.length).toBeGreaterThan(0);

    for (const sample of observed.samples) expect(sample).toEqual(observed.baseline);
    expect(observed.outsideMutations).toEqual([]);
    expect(observed.removedButtons).toBe(false);

    await expect(submitButton).toHaveAttribute("aria-disabled", "false");
    await expect(page.getByRole("status")).toHaveText("Imported log-viewer.json");
    await expect(page).toHaveURL(/\/runs$/);

    const font = await editor.evaluate((element) => getComputedStyle(element).fontFamily);
    expect(font).toContain("monospace");

    await editor.focus();
    await editor.press("Control+End");
    await expect.poll(() => editor.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
    const clipping = await editor.evaluate((element) => {
      const frame = element.parentElement!;
      const style = getComputedStyle(frame);
      const rect = element.getBoundingClientRect();
      const bounds = frame.getBoundingClientRect();
      return {
        overflows: element.scrollHeight > element.clientHeight,
        inside:
          rect.right < bounds.right && rect.left > bounds.left && rect.top > bounds.top && rect.bottom < bounds.bottom,
        overflow: style.overflow,
        radius: parseFloat(style.borderTopRightRadius)
      };
    });

    expect(clipping.overflows).toBe(true);
    expect(clipping.inside).toBe(true);
    expect(clipping.overflow).toBe("hidden");
    expect(clipping.radius).toBeGreaterThan(0);

    await editor.locator("..").screenshot({ path: testInfo.outputPath(`${colorScheme}-editor-overflow.png`) });
    await page.screenshot({ path: testInfo.outputPath(`${colorScheme}.png`) });
    expect((await visibleTextContrast(page)).failures).toEqual([]);

    const scan = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
      .analyze();
    expect(scan.violations).toEqual([]);

    await editor.fill(
      '{"id":"reviewed-file","root":{"id":"root","uses":"builtins/script","with":{"script":"echo reviewed"}}}'
    );

    await page.getByRole("button", { name: "Submit run", exact: true }).click();
    await expect(page).toHaveURL(/\/runs\/run-/);
    await expect(page.getByRole("heading", { level: 1 })).toContainText("reviewed-file");

    await page.goto("/jobs/create");
    await expect(page.getByLabel("Payload", { exact: true })).toBeVisible();

    expect(
      await page.getByLabel("Payload", { exact: true }).evaluate((element) => getComputedStyle(element).fontFamily)
    ).toBe(font);
  });
}

test("Run Once preserves the draft on invalid file import", async ({ page }) => {
  await page.goto("/runs");
  await page.getByRole("button", { name: "Run Once", exact: true }).click();
  const editor = page.getByLabel("Definition JSON", { exact: true });
  const previous = await editor.inputValue();

  await page
    .getByLabel("Choose definition JSON file")
    .setInputFiles({ name: "bad.json", mimeType: "application/json", buffer: Buffer.from("{broken") });
  await expect(page.getByRole("alert")).toContainText("The current definition has been kept.");
  await expect(editor).toHaveValue(previous);
  await expect(page).toHaveURL(/\/runs$/);
});

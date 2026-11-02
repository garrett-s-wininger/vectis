import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { readFile } from "node:fs/promises";
import { createServer, request } from "node:http";

for (const colorScheme of ["light", "dark"] as const) {
  for (const width of [1280, 390]) {
    test(`artifact panel layout and contrast ${colorScheme} ${width}px`, async ({ page }, testInfo) => {
      await page.emulateMedia({ colorScheme });
      await page.setViewportSize({ width, height: 900 });
      await page.goto("http://127.0.0.1:6101/iframe.html?id=pages-runartifacts--published&viewMode=story");
      await expect(page.getByRole("link", { name: "Download demo-report", exact: true })).toBeVisible({
        timeout: 20000
      });

      await expect(page.getByText("Report", { exact: true })).toBeVisible();
      await expect(page.getByText("Run-wide", { exact: true })).toBeVisible();
      await expect(page.getByText("0 B", { exact: true })).toBeVisible();

      const accessibility = await new AxeBuilder({ page })
        .include('[aria-label="Published artifacts"]')
        .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
        .analyze();

      expect(accessibility.violations).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
      await page.screenshot({ path: testInfo.outputPath("artifacts.png"), fullPage: true });
    });
  }
}

test("downloads exact bytes with the server filename without leaving run details", async ({ page }) => {
  const payload = "Vectis artifact demo\nAll demonstration checks passed.\n";
  const server = createServer((incoming, outgoing) => {
    if (incoming.url === "/api/v1/runs/artifact-demo-run/artifacts/demo-report/download") {
      outgoing.writeHead(200, {
        "Content-Type": "text/plain",
        "Content-Disposition": 'attachment; filename="report.txt"'
      });

      outgoing.end(payload);
      return;
    }

    const upstream = request(
      {
        hostname: "127.0.0.1",
        port: 6101,
        path: incoming.url,
        method: incoming.method,
        headers: { ...incoming.headers, host: "127.0.0.1:6101" }
      },
      (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(outgoing);
      }
    );

    upstream.on("error", () => {
      outgoing.writeHead(502);
      outgoing.end();
    });

    incoming.pipe(upstream);
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Download fixture failed to bind");

  try {
    await page.goto(`http://127.0.0.1:${address.port}/iframe.html?id=pages-run-detail--with-artifacts&viewMode=story`);
    const link = page.getByRole("link", { name: "Download demo-report", exact: true });
    await expect(link).toBeVisible({ timeout: 20000 });

    const original = page.url();
    const [download] = await Promise.all([page.waitForEvent("download"), link.click()]);
    expect(await readFile((await download.path())!, "utf8")).toBe(payload);
    expect(download.suggestedFilename()).toBe("report.txt");
    expect(page.url()).toBe(original);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test("shows empty, unavailable, and long-name artifact states without horizontal overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 900 });
  for (const [story, text] of [
    ["empty", "No artifacts published for this run."],
    ["unavailable", "Artifact service is unavailable. Try again shortly."],
    ["long-names", "test-results-and-build-outputs-for-the-complete-linux-windows-and-macos-release-matrix"]
  ]) {
    await page.goto(`http://127.0.0.1:6101/iframe.html?id=pages-runartifacts--${story}&viewMode=story`);
    await expect(page.getByText(text, { exact: true })).toBeVisible({ timeout: 20000 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  }
});

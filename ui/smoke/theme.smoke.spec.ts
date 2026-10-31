import { expect, test, type Page } from "@playwright/test";

async function themeColors(page: Page) {
  return page.evaluate(() => {
    const root = getComputedStyle(document.documentElement);
    const probe = document.createElement("span");
    document.body.append(probe);

    const pairs = [
      ["--text-primary", "--surface-page"],
      ["--text-muted", "--surface-panel"],
      ["--text-subtle", "--surface-panel"],
      ["--status-success-text", "--status-success-bg"],
      ["--status-warning-text", "--status-warning-bg"],
      ["--status-danger-text", "--status-danger-bg"]
    ].map(([foreground, background]) => {
      probe.style.color = `var(${foreground})`;
      probe.style.backgroundColor = `var(${background})`;
      const style = getComputedStyle(probe);

      return { name: foreground, foreground: style.color, background: style.backgroundColor };
    });

    const input = document.querySelector('input[name="jobName"]');
    if (input) {
      pairs.push({
        name: "input placeholder",
        foreground: getComputedStyle(input, "::placeholder").color,
        background: getComputedStyle(input).backgroundColor
      });
    }

    probe.remove();
    return { scheme: root.colorScheme, background: root.backgroundColor, pairs };
  });
}

function luminance(color: string) {
  const channels = color
    .match(/[\d.]+/g)
    ?.slice(0, 3)
    .map(Number);

  if (!channels || channels.length !== 3) throw new Error(`Unexpected color: ${color}`);
  const linear = channels.map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });

  return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
}

for (const preference of ["light", "dark", "no-preference"] as const) {
  test(`matches browser ${preference} preference with readable colors`, async ({ page }, testInfo) => {
    await page.emulateMedia({ colorScheme: preference });
    await page.goto("/jobs/create");
    await expect(page.getByRole("heading", { name: "Create Job" })).toBeVisible();

    const colors = await themeColors(page);
    expect(colors.scheme).toBe(preference === "dark" ? "dark" : "light");

    if (preference === "dark") expect(luminance(colors.background)).toBeLessThan(0.1);
    else expect(luminance(colors.background)).toBeGreaterThan(0.8);

    for (const pair of colors.pairs) {
      const a = luminance(pair.foreground);
      const b = luminance(pair.background);
      expect((Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05), pair.name).toBeGreaterThanOrEqual(4.5);
    }

    if (preference === "dark") {
      await expect(page.locator('img[aria-hidden="true"]').first()).toHaveJSProperty(
        "currentSrc",
        "http://127.0.0.1:5174/img/vectis-dark.png"
      );

      await page.screenshot({ path: testInfo.outputPath("job-editor-dark.png") });
    }
  });
}

test("updates the theme when the browser preference changes without losing form input", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/jobs/create");
  await page.getByLabel("Name", { exact: true }).fill("theme-change-job");
  await page.emulateMedia({ colorScheme: "dark" });
  await expect.poll(async () => (await themeColors(page)).scheme).toBe("dark");
  await page.emulateMedia({ colorScheme: "light" });
  await expect.poll(async () => (await themeColors(page)).scheme).toBe("light");
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("theme-change-job");
});

test("applies dark styling to login before authentication", async ({ page }, testInfo) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/login");
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  const input = page.getByLabel("Username");
  const style = await input.evaluate((element) => {
    const computed = getComputedStyle(element);
    return { foreground: computed.color, background: computed.backgroundColor };
  });
  expect(luminance(style.background)).toBeLessThan(0.1);
  expect(luminance(style.foreground)).toBeGreaterThan(0.5);
  await page.screenshot({ path: testInfo.outputPath("login-dark.png") });
});

async function chooseTheme(page: Page, value: "system" | "light" | "dark") {
  await page.getByLabel(/^Theme: /).click();
  await page
    .getByRole("listbox")
    .getByRole("option", { name: value[0].toUpperCase() + value.slice(1), exact: true })
    .click();
}

async function openThemePreferences(page: Page) {
  await page.locator("header").getByText("admin", { exact: true }).click();
  await expect(page.locator("header").getByLabel("Theme", { exact: true })).toHaveCount(0);
  await page.getByRole("link", { name: "Profile", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Preferences" })).toBeVisible();
  await expect(page.getByLabel(/^Theme: /)).toBeVisible();
}

test("saves an override across reloads and keeps it when the browser theme changes", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/jobs");
  await openThemePreferences(page);
  await chooseTheme(page, "light");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(page.locator('header img[aria-hidden="true"]').first()).toHaveJSProperty(
    "currentSrc",
    "http://127.0.0.1:5174/img/vectis.png"
  );
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(page.getByLabel("Theme", { exact: true })).toHaveValue("light");
  await page.emulateMedia({ colorScheme: "light" });
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await openThemePreferences(page);
  await chooseTheme(page, "system");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.reload();
  await expect(page.getByLabel("Theme", { exact: true })).toHaveValue("system");
});

test("applies a saved override before the application module loads", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.addInitScript(() => localStorage.setItem("vectis.ui.theme", "dark"));
  await page.route("**/src/main.tsx", (route) => route.abort());
  await page.goto("/login");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.locator('meta[name="color-scheme"]')).toHaveAttribute("content", "dark");
});

test("keeps the saved preference after sign-out clears browser storage", async ({ page, context, browser }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.route("**/ui/api/logout", (route) =>
    route.fulfill({ status: 204, headers: { "Clear-Site-Data": '"storage"' } })
  );
  await page.goto("/jobs");
  await openThemePreferences(page);
  await chooseTheme(page, "dark");
  await page.locator("header").getByText("admin", { exact: true }).click();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  const state = await context.storageState();
  expect(state.cookies.find((cookie) => cookie.name === "vectis_ui_theme")?.expires).toBeGreaterThan(Date.now() / 1000);
  const laterVisit = await browser.newContext({
    baseURL: "http://127.0.0.1:5174",
    colorScheme: "light",
    storageState: { cookies: state.cookies, origins: [] }
  });
  try {
    const laterPage = await laterVisit.newPage();
    await laterPage.goto("/login");
    await expect(laterPage.locator("html")).toHaveAttribute("data-theme", "dark");
  } finally {
    await laterVisit.close();
  }
});

test("shares changes across tabs and resets each tab to its own system preference", async ({ page, context }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/jobs");
  const second = await context.newPage();
  await second.emulateMedia({ colorScheme: "dark" });
  await second.goto("/profile");
  await expect(second.getByRole("heading", { name: "Preferences" })).toBeVisible();
  await openThemePreferences(page);
  await chooseTheme(page, "dark");
  await expect(second.getByLabel("Theme", { exact: true })).toHaveValue("dark");
  await chooseTheme(second, "system");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(second.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByLabel("Theme", { exact: true })).toHaveValue("system");
});

test("supports browser policies that disable localStorage", async ({ page }) => {
  await page.addInitScript(() =>
    Object.defineProperty(window, "localStorage", {
      get() {
        throw new Error("Storage unavailable");
      }
    })
  );
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/jobs");
  await openThemePreferences(page);
  await chooseTheme(page, "light");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
});

for (const width of [1280, 800, 640]) {
  test(`centers header controls below the accent at width ${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/jobs");
    const link = page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Jobs", exact: true });
    await expect(link).toBeVisible();
    const spacing = await link.evaluate((element) => {
      const header = element.closest("header")!;
      const bar = header.getBoundingClientRect();
      const control = element.getBoundingClientRect();
      const style = getComputedStyle(header);
      const accent = parseFloat(getComputedStyle(header, "::before").height);
      return {
        above: control.top - bar.top - accent,
        below: bar.bottom - parseFloat(style.borderBottomWidth) - control.bottom
      };
    });
    expect(Math.abs(spacing.above - spacing.below)).toBeLessThan(1);
  });
}

import { expect, type Page } from "@playwright/test";

export async function openNavigation(page: Page) {
  await page.getByRole("main").waitFor({ state: "visible" });
  const open = page.getByRole("button", { name: "Open navigation", exact: true });

  if (await open.isVisible()) {
    await open.click();
    await expect(page.getByRole("dialog", { name: "Navigation", exact: true })).toBeVisible();
  }
}

export async function closeNavigation(page: Page) {
  const close = page.getByRole("button", { name: "Close navigation", exact: true });
  if (await close.isVisible()) await close.click();
}

export async function navigatePrimary(page: Page, name: string) {
  await openNavigation(page);
  await page.getByRole("navigation", { name: "Primary", exact: true }).getByRole("link", { name, exact: true }).click();
}

export async function expectNamespace(page: Page, path: string) {
  await expect(page.getByRole("banner").getByTitle(path, { exact: true })).toBeVisible();
}

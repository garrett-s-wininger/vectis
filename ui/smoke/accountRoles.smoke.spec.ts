import { expect, test } from "@playwright/test";

test("creates a namespace-scoped operator and changes its role in place", async ({ page }) => {
  await page.goto("/users");
  await page.getByRole("button", { name: "Create", exact: true }).click();
  await page.getByLabel("Username", { exact: true }).fill("scoped-operator");
  await page.getByLabel("Initial Namespace", { exact: true }).selectOption("2");
  await page.getByLabel("Role", { exact: true }).selectOption("Operator");
  await page.getByRole("button", { name: "Add User", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "Initial Password" })).toBeVisible();

  await page.getByRole("button", { name: "Dismiss", exact: true }).click();
  await page.getByText("scoped-operator", { exact: true }).click();
  await page.getByLabel("Namespace", { exact: true }).selectOption("2");
  const bindings = page.getByRole("group", { name: "Role bindings", exact: true });
  await expect(bindings.getByText("Operator", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Update Role", exact: true })).toBeDisabled();

  await page.getByLabel("Role", { exact: true }).selectOption("Viewer");
  await page.getByRole("button", { name: "Update Role", exact: true }).click();
  await expect(bindings.getByText("Viewer", { exact: true })).toBeVisible();
  await expect(bindings.getByText("Operator", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Update Role", exact: true })).toBeDisabled();

  const originalWidth = (await page.getByRole("button", { name: "Remove User", exact: true }).boundingBox())!.width;
  await page.getByRole("button", { name: "Remove User", exact: true }).click();
  const confirm = page.getByRole("button", { name: "Confirm removal", exact: true });
  expect((await confirm.boundingBox())!.width).toBeCloseTo(originalWidth, 0);
  await expect(page.getByRole("dialog", { name: "Remove User" })).toHaveCount(0);

  await confirm.press("Escape");
  await page.getByRole("button", { name: "Remove User", exact: true }).click();
  await page.getByRole("button", { name: "Disable", exact: true }).focus();
  await expect(page.getByRole("button", { name: "Remove User", exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Remove User", exact: true }).click();
  await page.getByRole("button", { name: "Confirm removal", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Users", exact: true })).toBeVisible();
  await expect(page.getByText("scoped-operator", { exact: true })).toHaveCount(0);
});

test("changes an existing binding from the namespace page", async ({ page }) => {
  await page.goto("/namespaces/2");
  await page.getByLabel("User", { exact: true }).selectOption("user-mira");
  const bindings = page.getByRole("group", { name: "Namespace role bindings", exact: true });
  await expect(bindings.getByText("Operator", { exact: true })).toBeVisible();

  await page.getByLabel("Role", { exact: true }).selectOption("Viewer");
  await page.getByRole("button", { name: "Update Role", exact: true }).click();
  await expect(bindings.getByText("Viewer", { exact: true })).toBeVisible();
  await expect(bindings.getByText("Operator", { exact: true })).toHaveCount(0);
});

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import type { Result } from 'axe-core';

/** Routes to scan after production build (static HTML + hydration). */
const paths = ['/', '/docs/concepts/compatibility'];

function formatViolations(violations: Result[]): string {
  return violations
    .map((v) => {
      const nodes = v.nodes
        .slice(0, 5)
        .map((n) => `  - ${n.html.slice(0, 200)}`)
        .join('\n');
      const more = v.nodes.length > 5 ? `\n  … ${v.nodes.length - 5} more nodes` : '';
      return `${v.id} (${v.impact}): ${v.help}\n${nodes}${more}`;
    })
    .join('\n\n');
}

async function assertNoAxeViolations(page: import('@playwright/test').Page, path: string) {
  await page.goto(path, { waitUntil: 'load' });
  const results = await new AxeBuilder({ page }).analyze();
  expect(results.violations, formatViolations(results.violations)).toEqual([]);
}

test.describe('axe (light)', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('theme', 'light');
    });
  });

  for (const path of paths) {
    test(`${path}`, async ({ page }) => {
      await assertNoAxeViolations(page, path);
    });
  }
});

test.describe('axe (dark)', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('theme', 'dark');
    });
  });

  for (const path of paths) {
    test(`${path}`, async ({ page }) => {
      await assertNoAxeViolations(page, path);
    });
  }
});

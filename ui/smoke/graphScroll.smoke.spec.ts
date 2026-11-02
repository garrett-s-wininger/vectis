import { expect, test } from "@playwright/test";

for (const width of [1280, 390]) {
  test(`graph moves with its panel during rapid page scrolling at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 800 });
    await page.route("**/ui/api/context", (route) =>
      route.fulfill({ json: { auth_enabled: false, principal: { username: "admin" } } })
    );

    await page.goto("/runs/run-1238");
    const graph = page.getByRole("group", { name: "Execution graph", exact: true });
    await expect(graph).toBeVisible();

    await expect
      .poll(() =>
        graph.evaluate(
          (element) =>
            element.getAnimations({ subtree: true }).filter((animation) => animation.playState === "running").length
        )
      )
      .toBe(0);

    expect(
      await graph.evaluate((element) =>
        Array.from(element.querySelectorAll("button")).every(
          (button) => getComputedStyle(button.parentElement!).transform === "none"
        )
      )
    ).toBe(true);

    const node = graph.getByRole("button").first();
    const nodeOffset = async () => (await node.boundingBox())!.y - (await graph.boundingBox())!.y;
    const beforeHover = await nodeOffset();
    await node.hover();
    expect(await nodeOffset()).toBe(beforeHover);
    await expect(node).toHaveCSS("transform", "none");

    const box = (await graph.boundingBox())!;
    await page.mouse.move(box.x + box.width / 2, box.y + 8);
    const recording = page.evaluate(
      () =>
        new Promise<{ offset: number; scroll: number; pageScroll: number }[]>((resolve) => {
          const graph = document.querySelector('[aria-label="Execution graph"]')!;
          const node = graph.querySelector("button")!;
          const scroller = graph.closest("main")!;
          const samples: { offset: number; scroll: number; pageScroll: number }[] = [];
          const record = () => {
            samples.push({
              offset: node.getBoundingClientRect().y - graph.getBoundingClientRect().y,
              scroll: graph.scrollTop,
              pageScroll: scroller.scrollTop
            });

            if (samples.length < 90) requestAnimationFrame(record);
            else resolve(samples);
          };

          requestAnimationFrame(record);
        })
    );

    for (const delta of [180, 240, -160, -260, 320, -320]) await page.mouse.wheel(0, delta);
    const samples = await recording;
    expect(
      Math.max(...samples.map((sample) => sample.pageScroll)) - Math.min(...samples.map((sample) => sample.pageScroll))
    ).toBeGreaterThan(20);

    expect(
      Math.max(...samples.map((sample) => sample.offset)) - Math.min(...samples.map((sample) => sample.offset))
    ).toBeLessThanOrEqual(1);

    expect(samples.every((sample) => sample.scroll === 0)).toBe(true);

    if (width < 500) {
      expect(
        await graph.evaluate((element) => {
          element.scrollLeft = 100;
          return element.scrollLeft;
        })
      ).toBeGreaterThan(0);
    }
  });
}

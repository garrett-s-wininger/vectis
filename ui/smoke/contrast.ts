import type { Page } from "@playwright/test";

export function luminance(color: number[]) {
  const channels = color.map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });

  return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
}

export function contrastRatio(a: number[], b: number[]) {
  const [low, high] = [luminance(a), luminance(b)].sort((x, y) => x - y);
  return (high + 0.05) / (low + 0.05);
}

export async function visibleTextContrast(page: Page) {
  const text = await page.evaluate(() => {
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const items = [];
    let skippedOpacity = 0;

    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const parent = node.parentElement;
      if (!parent || !node.textContent?.trim() || parent.closest("script, style, svg, :disabled, [aria-disabled=true]"))
        continue;

      const style = getComputedStyle(parent);
      if (style.visibility !== "visible" || style.display === "none") continue;

      let opacity = 1;
      for (let ancestor: Element | null = parent; ancestor; ancestor = ancestor.parentElement)
        opacity *= Number(getComputedStyle(ancestor).opacity);

      if (opacity < 0.99) {
        skippedOpacity++;
        continue;
      }

      const foreground = style.color.match(/[\d.]+/g)?.map(Number);
      if (!foreground || (foreground.length > 3 && foreground[3] < 0.99)) continue;

      const range = document.createRange();
      range.selectNodeContents(node);
      const container = parent.getBoundingClientRect();

      for (const rect of range.getClientRects()) {
        const left = Math.max(0, rect.left, container.left);
        const right = Math.min(innerWidth, rect.right, container.right);
        const y = rect.top + rect.height / 2;
        if (right - left < 3 || y < 0 || y >= innerHeight) continue;

        const hit = document.elementFromPoint((left + right) / 2, y);
        if (!hit || !(parent.contains(hit) || hit.contains(parent))) continue;

        const size = parseFloat(style.fontSize);
        const large = size >= 24 || (size >= 18.667 && Number(style.fontWeight) >= 700);
        items.push({
          text: node.textContent.trim().slice(0, 100),
          foreground: foreground.slice(0, 3),
          threshold: large ? 3 : 4.5,
          points: [0.2, 0.5, 0.8].map((offset) => ({ x: left + (right - left) * offset, y }))
        });
      }
    }

    return { items, skippedOpacity };
  });

  const hidden = await page.addStyleTag({
    content:
      "*, *::before, *::after { -webkit-text-fill-color: transparent !important; color: transparent !important; text-shadow: none !important; }"
  });

  let screenshot: Buffer;

  try {
    screenshot = await page.screenshot({ animations: "disabled", caret: "hide" });
  } finally {
    await hidden.evaluate((node) => node.parentNode?.removeChild(node));
  }

  const backgrounds = await page.evaluate(
    async ({ png, items }) => {
      const bytes = Uint8Array.from(atob(png), (value) => value.charCodeAt(0));
      const image = await createImageBitmap(new Blob([bytes], { type: "image/png" }));
      const canvas = document.createElement("canvas");

      canvas.width = image.width;
      canvas.height = image.height;

      const context = canvas.getContext("2d")!;
      context.drawImage(image, 0, 0);

      const scale = image.width / innerWidth;
      const pixels = items.map((item) =>
        item.points.map((point) =>
          Array.from(context.getImageData(Math.floor(point.x * scale), Math.floor(point.y * scale), 1, 1).data).slice(
            0,
            3
          )
        )
      );

      image.close();
      return pixels;
    },
    { png: screenshot.toString("base64"), items: text.items }
  );

  const failures = text.items.flatMap((item, index) => {
    const ratio = Math.min(...backgrounds[index].map((background) => contrastRatio(item.foreground, background)));
    return ratio < item.threshold
      ? [
          {
            text: item.text,
            foreground: item.foreground,
            backgrounds: backgrounds[index],
            ratio,
            required: item.threshold
          }
        ]
      : [];
  });

  return { checked: text.items.length, skippedOpacity: text.skippedOpacity, failures };
}

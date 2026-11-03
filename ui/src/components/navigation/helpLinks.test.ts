import { helpLinks } from "./helpLinks";

it("preserves hosted docs base paths and keeps API documentation on the same site", () => {
  const links = helpLinks("https://docs.example.test/vectis/").items;
  expect(links[0].href).toBe("https://docs.example.test/vectis/docs/getting-started");
  expect(links[1].href).toBe("https://docs.example.test/vectis/api");
  expect(links.every((link) => link.external)).toBe(true);
});

it("supports same-origin docs paths and rejects executable URL schemes", () => {
  expect(helpLinks("/help").items[0].href).toBe(`${window.location.origin}/help/docs/getting-started`);
  expect(helpLinks("javascript:alert(1)").items[0].href).toBe("https://vectis.dev/docs/getting-started");
});

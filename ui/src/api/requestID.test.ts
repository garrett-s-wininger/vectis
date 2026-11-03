import { randomRequestID } from "./requestID";

afterEach(() => vi.unstubAllGlobals());

it("uses the native UUID implementation when available", () => {
  const native = vi.fn().mockReturnValue("native-id");
  vi.stubGlobal("crypto", { randomUUID: native });

  expect(randomRequestID()).toBe("native-id");
  expect(native).toHaveBeenCalledOnce();
});

it("creates a version 4 UUID using secure random bytes when randomUUID is unavailable", () => {
  const getRandomValues = vi.fn((bytes: Uint8Array) => {
    bytes.set(Array.from({ length: 16 }, (_, index) => index));
    return bytes;
  });

  vi.stubGlobal("crypto", { getRandomValues });
  expect(randomRequestID()).toBe("00010203-0405-4607-8809-0a0b0c0d0e0f");
  expect(getRandomValues).toHaveBeenCalledOnce();
});

it("does not substitute predictable randomness when secure randomness is unavailable", () => {
  vi.stubGlobal("crypto", undefined);
  expect(() => randomRequestID()).toThrow("cannot generate secure request IDs");
});

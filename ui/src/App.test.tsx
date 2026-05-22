import { fireEvent, render, screen } from "@testing-library/react";
import { App } from "./App";

describe("App", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    window.history.replaceState(null, "", "/");
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the setup route selected by the BFF", () => {
    window.history.replaceState(null, "", "/setup?next=%2Fruns%2F123");

    render(<App />);

    expect(
      screen.getByRole("heading", { name: "Complete setup" })
    ).toBeInTheDocument();
  });

  it("completes setup and navigates to next", async () => {
    window.history.replaceState(null, "", "/setup?next=%2Fruns%2F123");
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ username: "admin" }), { status: 200 })
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Bootstrap token"), {
      target: { value: "bootstrap-token" }
    });
    fireEvent.change(screen.getByLabelText("Admin username"), {
      target: { value: "admin" }
    });
    fireEvent.change(screen.getByLabelText("Admin password"), {
      target: { value: "password123" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Create admin" }));

    expect(
      await screen.findByRole("heading", { name: "Runs dashboard" })
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/runs/123");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/setup/complete",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("renders the login route selected by the BFF", () => {
    window.history.replaceState(null, "", "/login");

    render(<App />);

    expect(screen.getByRole("heading", { name: "Sign in" })).toBeInTheDocument();
  });

  it("logs in and navigates to next", async () => {
    window.history.replaceState(null, "", "/login?next=%2Fusers");
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ user_id: 1, username: "admin" }), {
        status: 200
      })
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" }
    });
    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "password123" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(
      await screen.findByRole("heading", { name: "Runs dashboard" })
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/users");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/login",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("ignores unsafe next values", async () => {
    window.history.replaceState(null, "", "/login?next=https%3A%2F%2Fevil.test");
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ user_id: 1, username: "admin" }), {
        status: 200
      })
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" }
    });
    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "password123" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(
      await screen.findByRole("heading", { name: "Runs dashboard" })
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/");
  });

  it("renders app routes directly after BFF authorization", () => {
    window.history.replaceState(null, "", "/runs/123");

    render(<App />);

    expect(
      screen.getByRole("heading", { name: "Runs dashboard" })
    ).toBeInTheDocument();
  });

  it("logs out and returns to login", async () => {
    window.history.replaceState(null, "", "/runs/123");
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));

    expect(
      await screen.findByRole("heading", { name: "Sign in" })
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/logout",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });
});

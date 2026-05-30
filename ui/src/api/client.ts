type APIErrorBody = {
  code?: string;
  message?: string;
};

export class VectisAPIError extends Error {
  code: string;
  status: number;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "VectisAPIError";
    this.status = status;
    this.code = code;
  }
}

export async function requestJSON<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: init.credentials ?? "same-origin",
    headers: requestHeaders(init, init.body !== undefined)
  });

  if (!response.ok) {
    throw await responseError(response);
  }

  return (await response.json()) as T;
}

export async function requestNoContent(path: string, init: RequestInit = {}) {
  const response = await fetch(path, {
    ...init,
    credentials: init.credentials ?? "same-origin",
    headers: requestHeaders(init, false)
  });

  if (!response.ok) {
    throw await responseError(response);
  }
}

function requestHeaders(init: RequestInit, hasJSONBody: boolean) {
  const headers = new Headers(init.headers);

  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }

  if (hasJSONBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  return headers;
}

async function responseError(response: Response) {
  let body: APIErrorBody = {};

  try {
    body = (await response.json()) as APIErrorBody;
  } catch {
    // Fall back to the generic API error when the body is empty or invalid JSON.
  }

  return new VectisAPIError(response.status, body.code ?? "request_failed", body.message ?? "Request failed");
}

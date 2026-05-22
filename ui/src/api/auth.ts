export type SetupCompleteRequest = {
  bootstrap_token: string;
  admin_username: string;
  admin_password: string;
};

export type SetupCompleteResponse = {
  username: string;
  expires_at?: string;
};

export type LoginRequest = {
  username: string;
  password: string;
};

export type LoginResponse = {
  user_id: number;
  username: string;
  expires_at?: string;
};

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

async function requestJSON<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers
    }
  });

  if (!response.ok) {
    let body: APIErrorBody = {};
    try {
      body = (await response.json()) as APIErrorBody;
    } catch {
      body = {};
    }

    throw new VectisAPIError(
      response.status,
      body.code ?? "request_failed",
      body.message ?? "Request failed"
    );
  }

  return (await response.json()) as T;
}

async function requestNoContent(path: string, init?: RequestInit) {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      ...init?.headers
    }
  });

  if (!response.ok) {
    let body: APIErrorBody = {};
    try {
      body = (await response.json()) as APIErrorBody;
    } catch {
      body = {};
    }

    throw new VectisAPIError(
      response.status,
      body.code ?? "request_failed",
      body.message ?? "Request failed"
    );
  }
}

export function completeSetup(payload: SetupCompleteRequest) {
  return requestJSON<SetupCompleteResponse>("/ui/api/setup/complete", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function login(payload: LoginRequest) {
  return requestJSON<LoginResponse>("/ui/api/login", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function logout() {
  return requestNoContent("/ui/api/logout", {
    method: "POST"
  });
}

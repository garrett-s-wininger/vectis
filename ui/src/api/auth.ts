import { requestJSON, requestNoContent, VectisAPIError } from "./client";

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

export type UIContext = {
  auth_enabled: boolean;
  principal: {
    kind: "auth_disabled" | "local_user";
    username: string;
  };
};

export { VectisAPIError };

export function loadUIContext() {
  return requestJSON<UIContext>("/ui/api/context");
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

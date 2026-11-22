import { posix, win32, resolve } from "node:path";
import { record, type AccessPolicy } from "./ingress.js";

export interface BridgeConfig extends AccessPolicy {
  serverURL: string;
  stateFile: string;
  maxOutputChars: number;
  reconcileMs: number;
  requestTimeoutMs: number;
  permissionTTLms: number;
}

export interface Credentials {
  botToken: string;
  appToken: string;
  serverPassword: string;
  serverUsername: string;
  historyToken?: string;
}

export function serverDirectory(value: string): string {
  if (/^[A-Za-z]:[\\/]|^[\\/]{2}[^\\/]/.test(value)) {
    const normalized = win32.normalize(value).replaceAll("\\", "/");
    return normalized.length === 3 ? normalized : normalized.replace(/\/$/, "");
  }
  if (posix.isAbsolute(value)) return posix.normalize(value).replace(/\/$/, "") || "/";
  throw new Error("Channel directory must be absolute on the OpenCode server");
}

export function sameDirectory(left: string, right: string): boolean {
  const a = serverDirectory(left), b = serverDirectory(right);
  return /^[A-Za-z]:|^\/\//.test(a) ? a.toLowerCase() === b.toLowerCase() : a === b;
}

function id(value: unknown, pattern: RegExp, field: string): string {
  if (typeof value !== "string" || !pattern.test(value)) throw new Error(`Invalid ${field}`);
  return value;
}

function integer(value: unknown, fallback: number, min: number, max: number, field: string): number {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) throw new Error(`Invalid ${field}`);
  return value;
}

export function parseConfig(value: unknown, baseDirectory = process.cwd()): BridgeConfig {
  const input = record(value);
  if (!input) throw new Error("Configuration must be an object");
  const fields = ["teamID", "appID", "users", "channels", "serverURL", "stateFile", "maxInputChars", "maxOutputChars", "reconcileMs", "requestTimeoutMs", "permissionTTLms"];
  if (Object.keys(input).some(key => !fields.includes(key))) throw new Error("Unknown configuration field; credentials belong in environment variables");
  const teamID = id(input.teamID, /^T[A-Z0-9]+$/, "teamID");
  const appID = id(input.appID, /^A[A-Z0-9]+$/, "appID");
  if (!Array.isArray(input.users) || input.users.length < 1 || input.users.length > 100) throw new Error("users must be a nonempty explicit allowlist");
  const users = [...new Set(input.users.map(user => id(user, /^[UW][A-Z0-9]+$/, "user ID")))];
  if (!Array.isArray(input.channels) || input.channels.length < 1 || input.channels.length > 100) throw new Error("channels must be a nonempty explicit route list");
  const channels = input.channels.map(value => {
    const route = record(value);
    if (!route || Object.keys(route).some(key => key !== "channelID" && key !== "directory")) throw new Error("Invalid channel route");
    const channelID = id(route.channelID, /^[CG][A-Z0-9]+$/, "channel ID");
    if (typeof route.directory !== "string" || /[\r\n\0]/.test(route.directory)) throw new Error("Invalid channel directory");
    return { channelID, directory: serverDirectory(route.directory) };
  });
  if (new Set(channels.map(route => route.channelID)).size !== channels.length) throw new Error("Duplicate channel route");
  if (typeof input.serverURL !== "string") throw new Error("serverURL is required");
  const url = new URL(input.serverURL);
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/" ||
      (url.protocol !== "https:" && !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))) {
    throw new Error("serverURL must be HTTPS or loopback HTTP, with no credentials, path, query or fragment");
  }
  if (typeof input.stateFile !== "string" || !input.stateFile.trim() || /[\r\n\0]/.test(input.stateFile)) throw new Error("stateFile is required");
  return {
    teamID, appID, users, channels, serverURL: url.origin, stateFile: resolve(baseDirectory, input.stateFile),
    maxInputChars: integer(input.maxInputChars, 8_000, 1, 32_000, "maxInputChars"),
    maxOutputChars: integer(input.maxOutputChars, 10_000, 256, 12_000, "maxOutputChars"),
    reconcileMs: integer(input.reconcileMs, 30_000, 1_000, 300_000, "reconcileMs"),
    requestTimeoutMs: integer(input.requestTimeoutMs, 10_000, 100, 60_000, "requestTimeoutMs"),
    permissionTTLms: integer(input.permissionTTLms, 600_000, 1_000, 3_600_000, "permissionTTLms"),
  };
}

export function loadCredentials(env: Readonly<Record<string, string | undefined>>): Credentials {
  const required = (name: string): string => {
    const value = env[name];
    if (!value?.trim() || /[\r\n\0]/.test(value)) throw new Error(`Missing or invalid ${name}`);
    return value;
  };
  const credentials = {
    botToken: required("SLACK_BOT_TOKEN"), appToken: required("SLACK_APP_TOKEN"),
    serverPassword: required("OPENCODE_SERVER_PASSWORD"), serverUsername: env.OPENCODE_SERVER_USERNAME ? required("OPENCODE_SERVER_USERNAME") : "opencode",
    ...(env.SLACK_HISTORY_TOKEN ? { historyToken: required("SLACK_HISTORY_TOKEN") } : {}),
  };
  if (credentials.serverUsername.includes(":")) throw new Error("Invalid OPENCODE_SERVER_USERNAME");
  return credentials;
}

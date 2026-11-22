import type { Credentials } from "./config.js";

export function redactor(credentials: Credentials): (text: string) => string {
  const secrets = [credentials.botToken, credentials.appToken, credentials.serverPassword, credentials.historyToken ?? "",
    Buffer.from(`${credentials.serverUsername}:${credentials.serverPassword}`).toString("base64")].filter(Boolean);
  return text => {
    for (const secret of secrets) text = text.replaceAll(secret, "[redacted]");
    return text.replace(/\b(?:xox[baprs]-[A-Za-z0-9-]+|xapp-[A-Za-z0-9-]+)\b/g, "[redacted]");
  };
}

export function bounded(text: string, limit: number): string {
  if (text.length <= limit) return text;
  const suffix = "\n[Truncated; open OpenCode for more.]";
  const end = limit - suffix.length;
  let result = text.slice(0, Math.max(0, end));
  if (/[\uD800-\uDBFF]$/.test(result)) result = result.slice(0, -1);
  return (result + suffix).slice(0, limit);
}

export function chunks(text: string, size = 2_800): string[] {
  if (!Number.isSafeInteger(size) || size < 2) throw new Error("Invalid chunk size");
  const result: string[] = [];
  while (text.length) {
    let end = Math.min(size, text.length);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1]!)) end--;
    result.push(text.slice(0, end));
    text = text.slice(end);
  }
  return result;
}

export const escapeSlack = (text: string): string => text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

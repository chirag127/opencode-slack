import { OpenCode, type OpenCodeClient, type PermissionRequest, type SessionInfo, type SessionMessageInfo } from "@opencode/client";
import type { BridgeConfig, Credentials } from "./config.js";
import { sameDirectory } from "./config.js";
import { record } from "./ingress.js";
import { hash, type Binding, type InputRow } from "./state.js";

export class RoutingError extends Error {
  constructor() { super("OpenCode session identity or directory changed"); }
}

export class ForeignSessionHistoryError extends RoutingError {}

const tag = (error: unknown): unknown => record(error)?._tag;

export function permissionFingerprint(request: PermissionRequest): string {
  const canonical = (value: unknown): unknown => Array.isArray(value) ? value.map(canonical)
    : record(value) ? Object.fromEntries(Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => [key, canonical(item)]))
    : value;
  return hash(canonical(request));
}

export class OpenCodeGateway {
  readonly client: OpenCodeClient;

  constructor(readonly config: BridgeConfig, credentials: Credentials, fetcher?: typeof fetch) {
    this.client = OpenCode.make({
      baseUrl: config.serverURL,
      headers: { authorization: `Basic ${Buffer.from(`${credentials.serverUsername}:${credentials.serverPassword}`).toString("base64")}` },
      ...(fetcher ? { fetch: fetcher } : {}),
    });
  }

  private options(signal: AbortSignal) {
    return { signal: AbortSignal.any([signal, AbortSignal.timeout(this.config.requestTimeoutMs)]) };
  }

  async check(signal: AbortSignal): Promise<void> {
    const info = await this.client.server.info(this.options(signal));
    if (!/^2\./.test(info.version)) throw new Error("OpenCode V2 is required");
  }

  private metadata(binding: Binding, bridgeID: string) {
    return { bridgeID, bindingKey: binding.key, teamID: binding.teamID, channelID: binding.channelID, threadTS: binding.threadTS, userID: binding.userID };
  }

  private verify(session: SessionInfo, binding: Binding, bridgeID: string): void {
    const metadata = record(session.metadata?.opencodeSlack);
    if (session.id !== binding.sessionID || session.parentID ||
        !session.location || !sameDirectory(session.location.directory, binding.directory) ||
        !metadata || Object.entries(this.metadata(binding, bridgeID)).some(([key, value]) => metadata[key] !== value)) throw new RoutingError();
  }

  async session(binding: Binding, bridgeID: string, signal: AbortSignal, create = false): Promise<SessionInfo> {
    if (binding.quarantined) throw new ForeignSessionHistoryError();
    let session: SessionInfo;
    try {
      session = await this.client.session.get({ sessionID: binding.sessionID }, this.options(signal));
    } catch (error) {
      if (tag(error) !== "SessionNotFoundError") throw error;
      if (!create || binding.established) throw new RoutingError();
      session = await this.client.session.create({
        id: binding.sessionID,
        title: `Slack ${binding.channelID} / ${binding.userID}`,
        location: { directory: binding.directory },
        metadata: { opencodeSlack: this.metadata(binding, bridgeID) },
      }, this.options(signal));
    }
    this.verify(session, binding, bridgeID);
    await this.verifyHistory(binding, signal);
    return session;
  }

  private async verifyHistory(binding: Binding, signal: AbortSignal): Promise<void> {
    let cursor: string | undefined;
    const seen = new Set<string>();
    // This independent filtered walk cannot skip moves behind a saved output cursor.
    for (let page = 0; page < 100; page++) {
      const result = await this.client.message.list({ sessionID: binding.sessionID, type: "location-switched", limit: 100,
        ...(cursor ? { cursor } : { order: "asc" }) }, this.options(signal));
      for (const message of result.data) {
        if (message.type !== "location-switched") throw new Error("Unexpected location history response");
        if (!sameDirectory(message.location.directory, binding.directory) ||
          (message.previous && !sameDirectory(message.previous.location.directory, binding.directory))) throw new ForeignSessionHistoryError();
      }
      const next = result.cursor.next;
      if (!next) return;
      if (seen.has(next)) throw new Error("Location history cursor did not advance");
      seen.add(next);
      cursor = next;
    }
    throw new Error("Location history exceeds the verification budget");
  }

  async prompt(binding: Binding, input: InputRow, signal: AbortSignal): Promise<void> {
    const text = input.inbound.text;
    const slack = { teamID: binding.teamID, channelID: binding.channelID, threadTS: binding.threadTS, userID: binding.userID, eventID: input.inbound.eventID };
    const matches = (metadata: unknown) => {
      const value = record(record(metadata)?.slack);
      return value && Object.entries(slack).every(([key, item]) => value[key] === item);
    };
    try {
      const result = await this.client.session.prompt({
        sessionID: binding.sessionID, id: input.messageID, text,
        delivery: input.inbound.delivery,
        metadata: { slack },
      }, this.options(signal));
      if (result.id !== input.messageID || result.sessionID !== binding.sessionID || result.payload.text !== text ||
          result.delivery !== input.inbound.delivery || !matches(result.payload.metadata)) throw new RoutingError();
    } catch (error) {
      if (tag(error) !== "ConflictError") throw error;
      const pending = await this.client.session.inbox.list({ sessionID: binding.sessionID }, this.options(signal));
      const item = pending.find(item => item.id === input.messageID);
      if (item) {
        if (item.type !== "user" || item.payload.text !== text || !matches(item.payload.metadata) || item.delivery !== input.inbound.delivery) throw new RoutingError();
        return;
      }
      const message = await this.client.session.message.get({ sessionID: binding.sessionID, messageID: input.messageID }, this.options(signal));
      if (message.id !== input.messageID || message.type !== "user" || message.text !== text || !matches(message.metadata)) throw new RoutingError();
    }
  }

  messages(binding: Binding, signal: AbortSignal) {
    // V2 cursors carry their order; supplying both makes the continuation invalid.
    return this.client.message.list({ sessionID: binding.sessionID, limit: 100,
      ...(binding.scanCursor ? { cursor: binding.scanCursor } : { order: "asc" }) }, this.options(signal));
  }

  permissions(binding: Binding, signal: AbortSignal): Promise<PermissionRequest[]> {
    return this.client.permission.list({ sessionID: binding.sessionID }, this.options(signal));
  }

  reply(binding: Binding, requestID: string, decision: "once" | "reject", signal: AbortSignal): Promise<void> {
    return this.client.permission.reply({ sessionID: binding.sessionID, requestID, decision }, this.options(signal));
  }
}

export function responseText(message: SessionMessageInfo): string | undefined {
  if (message.type === "idle") {
    return message.outcome === "succeeded" ? "OpenCode finished this turn."
      : message.outcome === "interrupted" ? "OpenCode was interrupted. Continue in this thread when ready."
      : "OpenCode reported a failure. Review the session locally for details.";
  }
  if (message.type !== "assistant" || message.time.completed === undefined || message.finish === "tool-calls") return;
  const text = message.content.filter(part => part.type === "text").map(part => part.text).join("\n").trim();
  return text || undefined;
}

export const invalidCursor = (error: unknown): boolean => tag(error) === "InvalidCursorError";

import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { resolve, join } from "node:path";
import { WebSocketServer, type WebSocket } from "ws";
import type { PermissionRequest, SessionInfo, SessionMessageInfo } from "@opencode/client";
import type { webApi } from "@slack/bolt";
import { parseConfig, type BridgeConfig, type Credentials } from "../src/config.js";

export const credentials: Credentials = {
  botToken: "xoxb-synthetic-bot", appToken: "xapp-synthetic-app",
  serverPassword: "synthetic-server-password", serverUsername: "opencode",
};
export const rootTS = "1795301000.000001";

export function temporary() {
  const root = resolve(".state", "test");
  mkdirSync(root, { recursive: true });
  const directory = mkdtempSync(join(root, "bridge-"));
  return { directory, remove: () => rmSync(directory, { recursive: true, force: true }) };
}

export function configuration(directory: string, serverURL = "http://127.0.0.1:40899"): BridgeConfig {
  return parseConfig({
    teamID: "TDEMO", appID: "ADEMO", users: ["UALICE", "UBOB"],
    channels: [{ channelID: "CPROJECT", directory: "/workspace/project" }, { channelID: "CSECOND", directory: "/workspace/second" }],
    serverURL, stateFile: join(directory, "state.sqlite"), reconcileMs: 1_000, requestTimeoutMs: 2_000,
  });
}

export function envelope(event: Record<string, unknown> = {}, body: Record<string, unknown> = {}) {
  return {
    type: "event_callback", api_app_id: "ADEMO", team_id: "TDEMO", event_id: "Ev1",
    authorizations: [{ enterprise_id: null, team_id: "TDEMO", user_id: "UBRIDGE", is_bot: true, is_enterprise_install: false }],
    event: { type: "app_mention", user: "UALICE", channel: "CPROJECT", ts: "1795301000.000002", thread_ts: rootTS,
      text: "<@UBRIDGE> Fix the parser", ...event }, ...body,
  };
}

export function interaction(nonce: string, ts: string, overrides: Record<string, unknown> = {}) {
  return {
    type: "block_actions", api_app_id: "ADEMO", team: { id: "TDEMO", domain: "synthetic" },
    user: { id: "UALICE", team_id: "TDEMO" }, channel: { id: "CPROJECT" },
    container: { type: "message", channel_id: "CPROJECT", message_ts: ts, is_ephemeral: false },
    message: { ts, thread_ts: rootTS },
    actions: [{ type: "button", block_id: "opencode_slack_permission", action_id: "opencode_slack_allow", value: nonce, action_ts: "1795301001.000001" }],
    ...overrides,
  };
}

export function changedUntil<T>(emitter: EventEmitter, read: () => T, predicate: (value: T) => boolean, timeout = 12_000): Promise<T> {
  const timeoutError = new Error("Fixture condition timed out");
  const initial = read();
  if (predicate(initial)) return Promise.resolve(initial);
  return new Promise((fulfill, reject) => {
    const finish = (value?: T, error?: Error) => {
      clearTimeout(timer); emitter.off("change", check);
      if (error) reject(error); else fulfill(value!);
    };
    const check = () => { const value = read(); if (predicate(value)) finish(value); };
    const timer = setTimeout(() => finish(undefined, timeoutError), timeout);
    emitter.on("change", check);
    check();
  });
}

async function bodyOf(request: IncomingMessage): Promise<Record<string, any>> {
  let body = "";
  for await (const chunk of request) body += chunk.toString();
  if (!body) return {};
  if (request.headers["content-type"]?.includes("application/json")) return JSON.parse(body);
  const parsed: Record<string, any> = Object.fromEntries(new URLSearchParams(body));
  for (const key of ["blocks", "metadata"]) if (typeof parsed[key] === "string") parsed[key] = JSON.parse(parsed[key]);
  return parsed;
}

function json(response: ServerResponse, value: unknown, status = 200) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}

const emptyTokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } };

/** A synthetic HTTP service; requests still pass through the published V2 client. */
export class OpenCodeFixture extends EventEmitter {
  readonly sessions = new Map<string, SessionInfo>();
  readonly messages = new Map<string, SessionMessageInfo[]>();
  readonly prompts = new Map<string, Record<string, any>>();
  readonly creates: Record<string, any>[] = [];
  readonly replies: Record<string, any>[] = [];
  readonly pending = new Map<string, PermissionRequest[]>();
  readonly requests: { path: string; query: string; method: string; body: Record<string, any> }[] = [];
  readonly messagePages: { sessionID: string; cursor: string | null; order: string | null; type: string | null; status: number; count?: number; next?: string }[] = [];
  readonly streams = new Set<ServerResponse>();
  private readonly server = createServer((req, res) => { void this.handle(req, res).catch(() => res.destroy()); });
  url = "";
  offline = false;
  promptFault: "none" | "before" | "after" = "none";
  createLostReply = false;
  invalidCursorOnce = false;
  streamCount = 0;
  blockedPermissionRead: Promise<void> | undefined;
  replyLostOnce = false;

  async start(): Promise<void> {
    this.server.listen(0, "127.0.0.1"); await once(this.server, "listening");
    const address = this.server.address(); assert.ok(address && typeof address !== "string");
    this.url = `http://127.0.0.1:${address.port}`;
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    assert.equal(req.headers.authorization, `Basic ${Buffer.from("opencode:synthetic-server-password").toString("base64")}`);
    const url = new URL(req.url!, this.url), path = url.pathname;
    const body = await bodyOf(req);
    this.requests.push({ path, query: url.search, method: req.method!, body }); this.emit("change");
    if (this.offline) { json(res, { error: "offline" }, 503); return; }
    if (path === "/api/info") { json(res, { version: "2.3.2", pid: 1, urls: [this.url], paths: { tmp: "/synthetic" } }); return; }
    if (path === "/api/event") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write(`data: ${JSON.stringify({ type: "server.connected", id: `evt_connect_${++this.streamCount}`, data: {} })}\n\n`);
      this.streams.add(res); req.on("close", () => { this.streams.delete(res); this.emit("change"); });
      this.emit("change"); return;
    }
    if (path === "/api/session" && req.method === "POST") {
      this.creates.push(body);
      if (this.sessions.has(body.id)) { json(res, { _tag: "InvalidRequestError", message: "ID exists" }, 400); return; }
      const session: SessionInfo = { id: body.id, projectID: "proj_test", title: body.title, agent: "build",
        cost: 0, tokens: emptyTokens, time: { created: Date.now(), updated: Date.now() },
        location: body.location, metadata: body.metadata };
      this.sessions.set(session.id, session); this.messages.set(session.id, []); this.emit("change");
      if (this.createLostReply) { this.createLostReply = false; res.destroy(); return; }
      json(res, { data: session }); return;
    }
    const match = /^\/api\/session\/([^/]+)(.*)$/.exec(path);
    if (!match) { json(res, { error: "Unexpected fixture endpoint" }, 500); return; }
    const sessionID = decodeURIComponent(match[1]!), suffix = match[2];
    const session = this.sessions.get(sessionID);
    if (!session) { json(res, { _tag: "SessionNotFoundError", sessionID, message: "Missing synthetic session" }, 404); return; }
    if (!suffix && req.method === "GET") { json(res, { data: session }); return; }
    if (suffix === "/prompt") {
      if (this.promptFault === "before") { this.promptFault = "none"; res.destroy(); return; }
      if (this.prompts.has(body.id)) { json(res, { _tag: "ConflictError", message: "Already admitted" }, 409); return; }
      this.prompts.set(body.id, { ...body, sessionID });
      this.messages.get(sessionID)!.push({ id: body.id, type: "user", time: { created: Date.now() }, text: body.text, metadata: body.metadata });
      this.emit("change");
      if (this.promptFault === "after") { this.promptFault = "none"; res.destroy(); return; }
      json(res, { data: { id: body.id, sessionID, type: "user", time: { created: Date.now() }, delivery: body.delivery,
        payload: { text: body.text, metadata: body.metadata } } }); return;
    }
    if (suffix === "/inbox") { json(res, { data: [] }); return; }
    if (suffix?.startsWith("/message/")) {
      const messageID = decodeURIComponent(suffix.slice(9));
      const message = this.messages.get(sessionID)?.find(message => message.id === messageID);
      if (!message) json(res, { _tag: "MessageNotFoundError", messageID, sessionID, message: "Missing" }, 404);
      else json(res, { data: message });
      return;
    }
    if (suffix === "/message") {
      const cursor = url.searchParams.get("cursor"), order = url.searchParams.get("order"), type = url.searchParams.get("type");
      if (cursor !== null && order !== null) {
        this.messagePages.push({ sessionID, cursor, order, type, status: 400 }); this.emit("change");
        json(res, { _tag: "InvalidCursorError", message: "Cursor cannot be combined with order" }, 400); return;
      }
      if (this.invalidCursorOnce) { this.invalidCursorOnce = false; json(res, { _tag: "InvalidCursorError", message: "Expired" }, 400); return; }
      const match = cursor === null ? undefined : /^fixture-page-(\d+)$/.exec(cursor);
      if (cursor !== null && !match) { json(res, { _tag: "InvalidCursorError", message: "Unknown fixture cursor" }, 400); return; }
      const offset = match ? Number(match[1]) : 0, limit = Number(url.searchParams.get("limit") ?? 100);
      const messages = this.messages.get(sessionID)!.filter(message => type === null || message.type === type);
      const data = messages.slice(offset, offset + limit);
      // OpenCode 2.3.2 supplies a next cursor even on a short nonempty last page.
      const next = data.length ? `fixture-page-${offset + data.length}` : undefined;
      this.messagePages.push({ sessionID, cursor, order, type, status: 200, count: data.length, ...(next ? { next } : {}) }); this.emit("change");
      json(res, { data, cursor: next ? { next } : {} }); return;
    }
    if (suffix === "/permission" && req.method === "GET") {
      await this.blockedPermissionRead;
      json(res, { data: this.pending.get(sessionID) ?? [] }); return;
    }
    if (/^\/permission\/[^/]+\/reply$/.test(suffix!)) {
      const requestID = decodeURIComponent(suffix!.split("/")[2]!);
      const requests = this.pending.get(sessionID) ?? [];
      if (!requests.some(request => request.id === requestID)) { json(res, { _tag: "InvalidRequestError", message: "Not pending" }, 400); return; }
      this.pending.set(sessionID, requests.filter(request => request.id !== requestID));
      this.replies.push({ ...body, sessionID, requestID }); this.emit("change");
      if (this.replyLostOnce) { this.replyLostOnce = false; res.destroy(); return; }
      res.writeHead(204); res.end(); return;
    }
    json(res, { error: "Unexpected fixture endpoint" }, 500);
  }

  emitEvent(sessionID: string, type = "session.execution.succeeded"): void {
    for (const response of this.streams) response.write(`data: ${JSON.stringify({ type, id: "evt_synthetic", created: Date.now(), data: { sessionID } })}\n\n`);
  }
  finish(sessionID: string, text: string, id = `msg_answer_${this.messages.get(sessionID)!.length}`, emit = true): void {
    this.messages.get(sessionID)!.push({ id, type: "assistant", agent: "build", model: { providerID: "synthetic", id: "no-inference" },
      time: { created: Date.now(), completed: Date.now() }, content: [{ type: "text", text }], finish: "stop" });
    this.emit("change"); if (emit) this.emitEvent(sessionID);
  }
  ask(sessionID: string, requestID = "perm_example", metadata?: Record<string, any>): PermissionRequest {
    const request: PermissionRequest = { id: requestID, sessionID, action: "shell", resources: ["npm test"],
      source: { type: "tool", messageID: "msg_tool", id: "tool_example" }, ...(metadata ? { metadata } : {}) };
    this.pending.set(sessionID, [request]); this.emitEvent(sessionID, "permission.asked"); return request;
  }
  dropStreams(): void { for (const res of this.streams) res.end(); }
  waitPrompts(count: number) { return changedUntil(this, () => [...this.prompts.values()], rows => rows.length >= count); }
  async stop(): Promise<void> {
    for (const res of this.streams) res.destroy();
    await new Promise<void>(resolve => { this.server.close(() => resolve()); this.server.closeAllConnections(); });
  }
}

export interface SlackPost {
  channel: string; thread_ts: string; ts: string; text: string;
  blocks: Record<string, any>[]; metadata: { event_type: string; event_payload: { bridgeID: string; deliveryID: string } };
  user: string; bot_id: string;
}

/** Real WebSocket and HTTP endpoints under the real Bolt/WebClient serialization boundary. */
export class SlackFixture extends EventEmitter {
  readonly transportResults: { method: string; elapsed: number; status?: number; error?: string }[] = [];
  readonly fixtureErrors: string[] = [];
  private readonly server = createServer((req, res) => { void this.handle(req, res).catch(error => {
    this.fixtureErrors.push(error instanceof Error ? error.name : "unknown"); res.destroy();
  }); });
  private readonly sockets = new Set<WebSocket>();
  private wss: WebSocketServer | undefined;
  private ackWaiters = new Map<string, () => void>();
  private nextEnvelope = 0;
  apiURL = "";
  wsURL = "";
  authTeam = "TDEMO";
  botApp = "ADEMO";
  posts: SlackPost[] = [];
  requests: { method: string; body: Record<string, any>; token: string | undefined }[] = [];
  connectionCount = 0;
  openFaults: ("transport" | "http")[] = [];
  blockOpen: Promise<void> | undefined;
  postFault: "none" | "after" | "before" | "rate" | "internal" = "none";
  blockPost: Promise<void> | undefined;
  blockPostResponse: Promise<void> | undefined;
  historyPageSize = 100;

  async start(): Promise<void> {
    this.server.listen(0, "127.0.0.1"); await once(this.server, "listening");
    const address = this.server.address(); assert.ok(address && typeof address !== "string");
    this.apiURL = `http://127.0.0.1:${address.port}`;
    this.wss = new WebSocketServer({ server: this.server }); this.wsURL = `ws://127.0.0.1:${address.port}/socket`;
    this.wss.on("connection", socket => {
      this.sockets.add(socket); this.connectionCount++; this.emit("change");
      socket.on("message", data => {
        const ack = JSON.parse(data.toString()) as { envelope_id: string };
        this.ackWaiters.get(ack.envelope_id)?.(); this.ackWaiters.delete(ack.envelope_id);
      });
      socket.on("close", () => { this.sockets.delete(socket); this.emit("change"); });
      socket.send(JSON.stringify({ type: "hello", num_connections: 1 }));
    });
  }

  readonly fetch: webApi.FetchFunction = async (url, init) => {
    const target = new URL(String(url));
    assert.equal(target.origin, "https://slack.com", "Fixture forbids calls to other Slack origins");
    const started = Date.now();
    try {
      const response = await globalThis.fetch(`${this.apiURL}${target.pathname}`, init as RequestInit);
      this.transportResults.push({ method: target.pathname, elapsed: Date.now() - started, status: response.status });
      return response;
    } catch (error) {
      this.transportResults.push({ method: target.pathname, elapsed: Date.now() - started, error: error instanceof Error ? error.name : "unknown" });
      throw error;
    }
  };

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const method = new URL(req.url!, this.apiURL).pathname.replace("/api/", ""), body = await bodyOf(req);
    this.requests.push({ method, body, token: req.headers.authorization }); this.emit("change");
    if (method === "auth.test") { json(res, { ok: true, team_id: this.authTeam, bot_id: "BBRIDGE", user_id: "UBRIDGE" }); return; }
    if (method === "bots.info") { json(res, { ok: true, bot: { id: "BBRIDGE", app_id: this.botApp, user_id: "UBRIDGE", deleted: false } }); return; }
    if (method === "apps.connections.open") {
      const fault = this.openFaults.shift();
      if (fault === "transport") { res.destroy(); return; }
      if (fault === "http") { json(res, { ok: false, error: "unavailable" }, 503); return; }
      await this.blockOpen;
      json(res, { ok: true, url: this.wsURL }); return;
    }
    if (method === "chat.postMessage") {
      const fault = this.postFault; this.postFault = "none";
      if (fault === "rate") { res.writeHead(429, { "retry-after": "1", "content-type": "application/json" }); res.end('{"ok":false,"error":"ratelimited"}'); return; }
      if (fault === "before") { res.destroy(); return; }
      await this.blockPost;
      const post = { ...body, ts: `1795302000.${String(this.posts.length + 1).padStart(6, "0")}`, user: "UBRIDGE", bot_id: "BBRIDGE" } as SlackPost;
      this.posts.push(post); this.emit("change");
      await this.blockPostResponse;
      if (fault === "after") { res.destroy(); return; }
      if (fault === "internal") { json(res, { ok: false, error: "internal_error" }); return; }
      json(res, { ok: true, channel: post.channel, ts: post.ts, message: post }); return;
    }
    if (method === "conversations.replies") {
      const offset = Number(body.cursor ?? 0), size = this.historyPageSize;
      const messages = this.posts.filter(post => post.channel === body.channel && post.thread_ts === body.ts);
      json(res, { ok: true, messages: messages.slice(offset, offset + size), has_more: offset + size < messages.length,
        response_metadata: { next_cursor: offset + size < messages.length ? String(offset + size) : "" } }); return;
    }
    json(res, { ok: false, error: "unexpected_fixture_method" });
  }

  send(body: Record<string, unknown>, type = "events_api"): Promise<void> {
    const envelopeID = `envelope-${++this.nextEnvelope}`;
    const socket = [...this.sockets].at(-1); assert.ok(socket);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.ackWaiters.delete(envelopeID); reject(new Error("Socket Mode ack timed out")); }, 3_000);
      this.ackWaiters.set(envelopeID, () => { clearTimeout(timer); resolve(); });
      socket.send(JSON.stringify({ envelope_id: envelopeID, type, payload: body, accepts_response_payload: type !== "events_api" }));
    });
  }
  raw(data: string): void { [...this.sockets].at(-1)!.send(data); }
  refresh(): void { [...this.sockets].at(-1)!.send(JSON.stringify({ type: "disconnect", reason: "refresh_requested" })); }
  dropConnections(): void { for (const socket of this.sockets) socket.terminate(); }
  receipt(post: SlackPost): Promise<void> { return this.send(envelope({ ...post, type: "message", subtype: "bot_message" }, { event_id: "EvReceipt" })); }
  waitPost(predicate: (post: SlackPost) => boolean) { return changedUntil(this, () => this.posts.find(predicate), post => post !== undefined) as Promise<SlackPost>; }
  waitConnections(count: number) { return changedUntil(this, () => this.connectionCount, value => value >= count); }
  get activeSockets(): number { return this.sockets.size; }
  async stop(): Promise<void> {
    for (const socket of this.sockets) socket.terminate();
    if (this.wss) await new Promise<void>(resolve => this.wss!.close(() => resolve()));
    await new Promise<void>(resolve => { this.server.close(() => resolve()); this.server.closeAllConnections(); });
  }
}

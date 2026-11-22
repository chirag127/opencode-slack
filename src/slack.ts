import { setTimeout as delay } from "node:timers/promises";
import { App, SocketModeReceiver, LogLevel, webApi, type Logger, type Receiver, type ReceiverEvent, type SocketModeReceiverOptions } from "@slack/bolt";
import type { BridgeConfig, Credentials } from "./config.js";
import { record, timestamp } from "./ingress.js";
import type { Binding, Outbound } from "./state.js";
import { chunks, escapeSlack } from "./text.js";

export interface SlackIdentity { botID: string; userID: string }
export interface SlackHandlers {
  admit(body: unknown): void;
  connected(): void;
  notice(message: string): void;
}
export interface SlackTransportOptions {
  fetch?: webApi.WebClientOptions["fetch"];
  dispatcher?: SocketModeReceiverOptions["dispatcher"];
}

// Bolt/Socket Mode debug logs may contain entire envelopes and credentials.
function safeLogger(notice: SlackHandlers["notice"]): Logger {
  return { getLevel: () => LogLevel.ERROR, setLevel() {}, setName() {}, debug() {}, info() {},
    warn: () => notice("Slack transport warning"), error: () => notice("Slack transport error") };
}

class AdmissionReceiver implements Receiver {
  constructor(readonly socket: SocketModeReceiver, private beforeAck: (body: unknown) => void) {}
  init(app: App): void {
    this.socket.init(new Proxy(app, {
      get: (target, property, receiver) => property === "processEvent"
        ? async (event: ReceiverEvent) => {
          let admitted = false;
          await target.processEvent({ ...event, ack: async response => {
            if (!admitted) {
              this.beforeAck(event.body);
              admitted = true;
            }
            await event.ack(response);
          } });
        }
        : Reflect.get(target, property, receiver),
    }));
  }
  start(): Promise<unknown> { return this.socket.start(); }
  // Bolt 5.1.0's receiver.stop discards disconnect's promise. Await the public client instead.
  async stop(): Promise<void> { await this.socket.client.disconnect(); }
}

export class SlackTransport {
  readonly app: App;
  readonly socket: SocketModeReceiver;
  readonly client: webApi.WebClient;
  private historyClient: webApi.WebClient | undefined;
  private identityValue: SlackIdentity | undefined;
  private authorized = new WeakSet<object>();
  private controller = new AbortController();
  private stopped = false;
  private started = false;
  private connected = false;
  private reconnectTask: Promise<void> | undefined;
  private stopTask: Promise<void> | undefined;
  private onConnected: () => void;
  private onDisconnected: () => void;

  constructor(private config: BridgeConfig, private credentials: Credentials, private handlers: SlackHandlers,
    options: SlackTransportOptions = {}) {
    const logger = safeLogger(handlers.notice);
    const network = options.fetch ?? globalThis.fetch;
    const fetcher: webApi.FetchFunction = (url, init) => network(url, { ...init, redirect: "error",
      signal: AbortSignal.any([this.controller.signal, ...(init?.signal ? [init.signal] : []), AbortSignal.timeout(config.requestTimeoutMs)]) });
    const clientOptions: webApi.WebClientOptions = { fetch: fetcher, timeout: config.requestTimeoutMs,
      retryConfig: { retries: 0 }, rejectRateLimitedCalls: true, allowAbsoluteUrls: false, logger };
    this.client = new webApi.WebClient(credentials.botToken, clientOptions);
    if (credentials.historyToken) this.historyClient = new webApi.WebClient(credentials.historyToken, clientOptions);
    // Socket Mode's async reconnect listeners let HTTP failures escape; supervise public start() instead.
    this.socket = new SocketModeReceiver({ appToken: credentials.appToken, logger, autoReconnectEnabled: false,
      ...(options.dispatcher ? { dispatcher: options.dispatcher } : {}),
      installerOptions: { clientOptions },
      processEventErrorHandler: async () => { handlers.notice("Slack envelope was not acknowledged; admission failed"); return false; } });
    const receiver = new AdmissionReceiver(this.socket, body => {
      if (this.stopped) throw new Error("Bridge is stopping");
      if (record(body) && this.authorized.has(body as object)) handlers.admit(body);
    });
    this.app = new App({ receiver, convoStore: false, ignoreSelf: false, attachFunctionToken: false,
      clientOptions, logger, authorize: async (source, body) => {
        const b = record(body);
        if (!this.identityValue || !b || source.isEnterpriseInstall || source.teamId !== config.teamID || b.api_app_id !== config.appID) {
          throw new Error("Slack envelope identity is not authorized");
        }
        this.authorized.add(b);
        return { botToken: credentials.botToken, botId: this.identityValue.botID,
          botUserId: this.identityValue.userID, teamId: config.teamID };
      } });
    // The receiver journals before Bolt's acknowledgement, including ignored bot receipts.
    this.app.event("app_mention", async () => {});
    this.app.event("message", async () => {});
    this.app.action(/^opencode_slack_(allow|reject)$/, async ({ ack }) => { await ack(); });
    this.app.error(async () => { handlers.notice("Slack event was rejected"); });
    this.onConnected = () => { this.connected = true; handlers.connected(); };
    this.onDisconnected = () => { this.connected = false; this.scheduleReconnect(); };
    this.socket.client.on("connected", this.onConnected);
    this.socket.client.on("disconnected", this.onDisconnected);
  }

  get identity(): SlackIdentity {
    if (!this.identityValue) throw new Error("Slack is not authenticated");
    return this.identityValue;
  }

  async start(): Promise<void> {
    const auth = await this.client.auth.test();
    if (auth.team_id !== this.config.teamID || !auth.bot_id || !auth.user_id) throw new Error("Slack bot token does not match the configured workspace");
    const info = await this.client.bots.info({ bot: auth.bot_id });
    if (!info.bot || info.bot.app_id !== this.config.appID || info.bot.deleted || info.bot.user_id !== auth.user_id) {
      throw new Error("Slack bot token does not match the configured app");
    }
    if (this.historyClient && (await this.historyClient.auth.test()).team_id !== this.config.teamID) {
      throw new Error("Slack history token does not match the configured workspace");
    }
    if (this.stopped) throw new Error("Bridge is stopping");
    this.identityValue = { botID: auth.bot_id, userID: auth.user_id };
    await this.app.start();
    if (this.stopped) throw new Error("Bridge is stopping");
    this.started = true;
    if (!this.connected) this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.stopped || !this.started || this.reconnectTask) return;
    this.reconnectTask = this.reconnect().finally(() => {
      this.reconnectTask = undefined;
      if (!this.stopped && !this.connected) this.scheduleReconnect();
    });
  }

  private async reconnect(): Promise<void> {
    let attempts = 0;
    while (!this.stopped && !this.connected) {
      try {
        await delay(Math.min(30_000, 1_000 * 2 ** Math.min(attempts++, 5)) + Math.floor(Math.random() * 250),
          undefined, { signal: this.controller.signal });
        if (this.stopped) return;
        await this.socket.start();
      } catch {
        if (this.stopped) return;
        this.handlers.notice("Slack connection could not reopen; retrying with backoff");
      }
    }
  }

  stop(): Promise<void> {
    if (this.stopTask) return this.stopTask;
    this.stopped = true;
    this.controller.abort();
    this.socket.client.removeListener("connected", this.onConnected);
    this.socket.client.removeListener("disconnected", this.onDisconnected);
    this.stopTask = (async () => {
      try { await this.app.stop(); }
      finally { await this.reconnectTask; }
    })();
    return this.stopTask;
  }

  async post(binding: Binding, row: Outbound, bridgeID: string): Promise<string> {
    const blocks: Extract<webApi.ChatPostMessageArguments, { blocks: unknown }>["blocks"] = chunks(row.text).map(text => ({
      type: "section", text: { type: "plain_text", text, emoji: false },
    }));
    if (row.permissionNonce) blocks.push({ type: "actions", block_id: "opencode_slack_permission", elements: [
      { type: "button", text: { type: "plain_text", text: "Allow once" }, action_id: "opencode_slack_allow", value: row.permissionNonce },
      { type: "button", text: { type: "plain_text", text: "Reject" }, action_id: "opencode_slack_reject", value: row.permissionNonce },
    ] });
    const response = await this.client.chat.postMessage({ channel: binding.channelID, thread_ts: binding.threadTS,
      reply_broadcast: false, text: escapeSlack(row.text), mrkdwn: false, parse: "none", link_names: false,
      unfurl_links: false, unfurl_media: false, blocks,
      metadata: { event_type: "opencode_slack_delivery", event_payload: { bridgeID, deliveryID: row.id } } });
    if (response.channel !== binding.channelID || !timestamp(response.ts)) throw new Error("Slack send response did not confirm its destination");
    return response.ts;
  }

  receipt(body: unknown, bridgeID: string): { id: string; channelID: string; threadTS: string; ts: string } | undefined {
    const b = record(body), event = record(b?.event);
    if (!b || b.team_id !== this.config.teamID || b.api_app_id !== this.config.appID || event?.type !== "message" ||
        event.hidden === true || (event.subtype !== undefined && event.subtype !== "bot_message")) return;
    return this.messageReceipt(event, bridgeID);
  }

  private messageReceipt(message: Record<string, unknown>, bridgeID: string) {
    const metadata = record(message.metadata), payload = record(metadata?.event_payload);
    if (!this.identityValue || message.user !== this.identityValue.userID || message.bot_id !== this.identityValue.botID ||
        metadata?.event_type !== "opencode_slack_delivery" || payload?.bridgeID !== bridgeID ||
        typeof payload.deliveryID !== "string" || typeof message.channel !== "string" ||
        !timestamp(message.thread_ts) || !timestamp(message.ts)) return;
    return { id: payload.deliveryID, channelID: message.channel, threadTS: message.thread_ts, ts: message.ts };
  }

  async recover(binding: Binding, row: Outbound, bridgeID: string): Promise<{ ts?: string; cursor: string | null }> {
    // Bot tokens cannot generally read channel threads. This optional read-only user token is never used for writes.
    if (!this.historyClient) return { cursor: null };
    const page = await this.historyClient.conversations.replies({ channel: binding.channelID, ts: binding.threadTS,
      include_all_metadata: true, limit: 100, ...(row.lookupCursor ? { cursor: row.lookupCursor } : {}) });
    for (const item of page.messages ?? []) {
      const receipt = this.messageReceipt({ ...item, channel: binding.channelID }, bridgeID);
      if (receipt?.id === row.id && receipt.threadTS === binding.threadTS) return { ts: receipt.ts, cursor: null };
    }
    return { cursor: page.response_metadata?.next_cursor || null };
  }
}

export function rejectedSend(error: unknown): { wait?: number } | undefined {
  if (error instanceof webApi.WebAPIRateLimitedError) return { wait: Math.max(1, error.retryAfter) * 1_000 };
  // An internal/platform timeout can arrive after a write. Only known rejections are safe to retry.
  if (error instanceof webApi.WebAPIPlatformError && ["ratelimited", "not_in_channel", "channel_not_found", "is_archived",
    "invalid_auth", "account_inactive", "token_revoked", "missing_scope", "no_permission", "invalid_arguments",
    "invalid_blocks", "msg_too_long"].includes(error.data.error ?? "")) return {};
  return undefined;
}

export function sendFailureCategory(error: unknown): string {
  if (error instanceof webApi.WebAPIRateLimitedError) return "rate-limit";
  if (error instanceof webApi.WebAPIPlatformError) return "platform";
  if (error instanceof webApi.WebAPIHTTPError) return "http";
  if (error instanceof webApi.WebAPIRequestError) return "transport";
  if (error instanceof Error && error.message === "Slack send response did not confirm its destination") return "destination-response";
  return "unknown";
}

import { setTimeout as delay } from "node:timers/promises";
import type { OpenCodeEvent, PermissionRequest } from "@opencode/client";
import { parseConfig, type BridgeConfig, type Credentials } from "./config.js";
import { parseInbound, permitted, record } from "./ingress.js";
import { ForeignSessionHistoryError, invalidCursor, OpenCodeGateway, permissionFingerprint, responseText, RoutingError } from "./opencode.js";
import { rejectedSend, sendFailureCategory, SlackTransport, type SlackTransportOptions } from "./slack.js";
import { State, type Binding, type Outbound, type PermissionGate } from "./state.js";
import { bounded, redactor } from "./text.js";

export interface BridgeOptions {
  openCodeFetch?: typeof fetch;
  slack?: SlackTransportOptions;
  notice?: (message: string) => void;
}

/** One explicitly owned Socket Mode process. Constructing it opens state, not network connections. */
export class SlackBridge {
  readonly config: BridgeConfig;
  private readonly store: State;
  private readonly gateway: OpenCodeGateway;
  private readonly slack: SlackTransport;
  private readonly redact: (text: string) => string;
  private readonly notice: (text: string) => void;
  private readonly controller = new AbortController();
  private phase: "new" | "starting" | "running" | "stopped" = "new";
  private startTask: Promise<void> | undefined;
  private stopTask: Promise<void> | undefined;
  private eventTask: Promise<void> | undefined;
  private work: Promise<void> | undefined;
  private timer: NodeJS.Timeout | undefined;
  private nextFullScan = 0;
  private readonly dirty = new Set<string>();
  private rerun = false;

  constructor(config: BridgeConfig, credentials: Credentials, options: BridgeOptions = {}) {
    this.config = parseConfig(config);
    this.notice = options.notice ?? (() => {});
    this.redact = redactor(credentials);
    this.store = new State(this.config);
    try {
      this.gateway = new OpenCodeGateway(this.config, credentials, options.openCodeFetch);
      this.slack = new SlackTransport(this.config, credentials, {
        admit: body => this.admit(body),
        connected: () => { this.nextFullScan = 0; this.kick(); },
        notice: text => this.notice(text),
      }, options.slack);
    } catch (error) {
      this.store.close();
      throw error;
    }
  }

  status(): { phase: string; counts: Record<string, number> } {
    return { phase: this.phase, counts: this.phase === "stopped" ? {} : this.store.counts() };
  }

  start(): Promise<void> {
    if (this.phase === "stopped") return Promise.reject(new Error("Bridge is stopped; create a new instance to restart"));
    if (this.startTask) return this.startTask;
    this.phase = "starting";
    this.startTask = this.startInternal();
    return this.startTask;
  }

  private async startInternal(): Promise<void> {
    try {
      await this.slack.start();
      if (this.controller.signal.aborted) throw new Error("Bridge is stopping");
      this.store.botIdentity(this.slack.identity.botID, this.slack.identity.userID);
      this.phase = "running";
      this.eventTask = this.watchEvents();
      await this.reconcile();
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  private admit(body: unknown): void {
    if (this.controller.signal.aborted) throw new Error("Bridge is stopping");
    this.store.botIdentity(this.slack.identity.botID, this.slack.identity.userID);
    const receipt = this.slack.receipt(body, this.store.bridgeID);
    if (receipt) {
      this.store.confirmReceipt(receipt.id, { ...receipt, teamID: this.config.teamID }, receipt.ts);
    } else if (record(body)?.type === "block_actions") {
      this.store.acceptInteraction(body, Date.now());
    } else {
      const input = parseInbound(body, this.config, this.slack.identity.userID);
      if (input) this.store.accept({ ...input, text: this.redact(input.text) });
    }
    // Admission is synchronous and durable; no model or Web API request delays the acknowledgment.
    this.kick();
  }

  private kick(): void {
    if (this.phase !== "running") return;
    if (this.work) { this.rerun = true; return; }
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => { this.timer = undefined; void this.reconcile(); }, 0);
  }

  /** Reconcile saved state with both services, also useful to an embedding host after connectivity changes. */
  reconcile(): Promise<void> {
    if (this.phase !== "running") return Promise.resolve();
    if (this.work) { this.rerun = true; return this.work; }
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.work = this.cycle().catch(() => {
      if (!this.controller.signal.aborted) this.notice("Bridge reconciliation failed; durable work will be retried");
    }).finally(() => {
      this.work = undefined;
      if (this.phase !== "running") return;
      const due = this.store.nextDue();
      const next = this.rerun ? 0 : Math.min(this.nextFullScan,
        due === undefined ? this.nextFullScan : Math.max(Date.now() + 1_100, due));
      this.rerun = false;
      this.timer = setTimeout(() => { this.timer = undefined; void this.reconcile(); }, Math.max(100, next - Date.now()));
    });
    return this.work;
  }

  private async cycle(): Promise<void> {
    const signal = this.controller.signal;
    const now = Date.now();
    try { await this.gateway.check(signal); }
    catch {
      if (!signal.aborted) this.notice("OpenCode is unavailable or incompatible; saved requests remain pending");
      this.nextFullScan = now + this.config.reconcileMs;
      return;
    }
    for (const row of this.store.inputs(now)) {
      if (signal.aborted) return;
      const binding = this.store.binding(row.bindingKey)!;
      try {
        if (!permitted(binding, this.config, binding.directory)) throw new RoutingError();
        await this.gateway.session(binding, this.store.bridgeID, signal, true);
        this.store.establish(binding.key);
        await this.gateway.prompt(binding, row, signal);
        this.store.inputDelivered(row);
        this.dirty.add(binding.key);
      } catch (error) {
        if (error instanceof ForeignSessionHistoryError) this.quarantine(binding);
        else if (error instanceof RoutingError) this.store.inputFailed(row);
        else this.store.inputRetry(row.id, now, row.attempts);
      }
    }
    for (const gate of this.store.gates(now)) {
      if (signal.aborted) return;
      await this.deliverDecision(gate, now);
    }
    const full = now >= this.nextFullScan;
    if (full) this.nextFullScan = now + this.config.reconcileMs;
    const bindings = full ? this.store.bindings() : [...this.dirty].map(key => this.store.binding(key)!).filter(Boolean);
    this.dirty.clear();
    for (const binding of bindings) {
      if (signal.aborted) return;
      if (!binding.established || binding.quarantined || !permitted(binding, this.config, binding.directory)) continue;
      try {
        await this.gateway.session(binding, this.store.bridgeID, signal);
        this.store.releaseHolds(binding.key);
        await this.scan(binding);
        await this.scanPermissions(binding, now);
      } catch (error) {
        if (error instanceof ForeignSessionHistoryError) this.quarantine(binding);
        else if (invalidCursor(error)) {
          this.store.cursor(binding.key, null);
          this.dirty.add(binding.key);
          this.rerun = true;
        } else if (error instanceof RoutingError) {
          this.notice("A saved Slack route no longer matches its OpenCode session; delivery is held");
        } else if (!signal.aborted) this.notice("Session reconciliation deferred until the next check");
      }
    }
    for (const row of this.store.outputs(now)) {
      if (signal.aborted) return;
      await this.deliverOutput(row, now);
    }
  }

  private async scan(binding: Binding): Promise<void> {
    const page = await this.gateway.messages(binding, this.controller.signal);
    for (const message of page.data) {
      const text = responseText(message);
      if (text) this.store.observe(binding.key, message.id, [bounded(this.redact(text), this.config.maxOutputChars)]);
    }
    // Finish each page walk, then revisit earlier messages that may have completed meanwhile.
    this.store.cursor(binding.key, page.cursor.next ?? null);
    if (page.cursor.next) { this.dirty.add(binding.key); this.rerun = true; }
  }

  private quarantine(binding: Binding): void {
    if (this.store.quarantine(binding.key)) this.notice("A saved Slack session has foreign directory history and is quarantined; use a new Slack thread.");
  }

  private async scanPermissions(binding: Binding, now: number): Promise<void> {
    for (const request of await this.gateway.permissions(binding, this.controller.signal)) {
      if (request.sessionID !== binding.sessionID) continue;
      const details = this.redact([
        `OpenCode asks ${binding.userID} to approve once or reject.`,
        `Session: ${binding.sessionID}`, `Action: ${request.action}`,
        ...request.resources, request.message ?? "",
        ...(request.source ? [`Source: ${JSON.stringify(request.source)}`] : []),
        ...(request.metadata ? [`Metadata: ${JSON.stringify(request.metadata)}`] : []),
      ].join("\n")).trim();
      if (details.length > this.config.maxOutputChars) {
        this.store.enqueue(`permission-local:${binding.key}:${request.id}`, binding.key,
          "A permission request is too long to review in Slack. Review and answer it in OpenCode.");
        continue;
      }
      this.store.gate(binding.key, request.id, permissionFingerprint(request), details, now);
    }
  }

  private async pending(binding: Binding, gate: PermissionGate): Promise<PermissionRequest | undefined> {
    const requests = await this.gateway.permissions(binding, this.controller.signal);
    return requests.find(request => request.sessionID === binding.sessionID && request.id === gate.requestID &&
      permissionFingerprint(request) === gate.fingerprint);
  }

  private async deliverDecision(gate: PermissionGate, now: number): Promise<void> {
    const binding = this.store.binding(gate.bindingKey)!;
    try {
      if (!permitted(binding, this.config, binding.directory) || gate.expires <= now || !gate.decision) {
        this.store.gateOutcome(gate, "Permission response expired or its route changed; review it in OpenCode.");
        return;
      }
      await this.gateway.session(binding, this.store.bridgeID, this.controller.signal);
      if (!await this.pending(binding, gate)) {
        this.store.gateOutcome(gate, "This exact permission is no longer pending. No new permission was approved.");
        return;
      }
      await this.gateway.reply(binding, gate.requestID, gate.decision, this.controller.signal);
      this.store.gateOutcome(gate, gate.decision === "once" ? "Your one-time permission decision was delivered." : "Your rejection was delivered.");
      this.dirty.add(binding.key);
    } catch (error) {
      if (error instanceof ForeignSessionHistoryError) this.quarantine(binding);
      else if (error instanceof RoutingError) this.store.gateDone(gate.nonce);
      else this.store.gateRetry(gate, now);
    }
  }

  private async deliverOutput(row: Outbound, now: number): Promise<void> {
    const binding = this.store.binding(row.bindingKey)!;
    try {
      if (binding.quarantined || !permitted(binding, this.config, binding.directory)) { this.store.hold(row.id); return; }
      await this.gateway.session(binding, this.store.bridgeID, this.controller.signal);
      if (row.status === "uncertain") {
        const receipt = await this.slack.recover(binding, row, this.store.bridgeID);
        if (receipt.ts) this.store.sent(row.id, receipt.ts);
        else { this.store.lookupCursor(row.id, receipt.cursor); this.store.deferOutput(row.id, now); }
        return;
      }
      if (row.permissionNonce) {
        const gate = this.store.getGate(row.permissionNonce);
        if (!gate || gate.status !== "ready" || gate.expires <= now || !await this.pending(binding, gate)) {
          this.store.cancelOutput(row.id);
          return;
        }
      }
      if (!this.store.reservePost(binding.channelID, now)) { this.store.outputDue(row.id, now + 1_100); return; }
      this.store.uncertain(row.id, now + 60_000);
      const started = Date.now();
      try {
        const ts = await this.slack.post(binding, row, this.store.bridgeID);
        this.store.sent(row.id, ts);
      } catch (error) {
        if (this.store.output(row.id)?.status === "sent") return;
        const rejection = rejectedSend(error);
        if (rejection) this.store.outputRetry(row, now, rejection.wait);
        else this.notice(`A Slack send is unconfirmed (${sendFailureCategory(error)}, ${Date.now() - started} ms); outbox reconciliation will not blindly repeat it`);
      }
    } catch (error) {
      if (error instanceof ForeignSessionHistoryError) this.quarantine(binding);
      else if (error instanceof RoutingError) this.store.hold(row.id);
      else this.store.deferOutput(row.id, now);
    }
  }

  private event(event: OpenCodeEvent): void {
    if (event.type === "server.connected") { this.nextFullScan = 0; this.kick(); return; }
    const data = record(record(event)?.data);
    const sessionID = data?.sessionID;
    if (typeof sessionID !== "string") return;
    const binding = this.store.bySession(sessionID);
    if (binding) { this.dirty.add(binding.key); this.kick(); }
  }

  private async watchEvents(): Promise<void> {
    let attempts = 0;
    const signal = this.controller.signal;
    while (!signal.aborted) {
      const stream = new AbortController();
      let activity = Date.now();
      const watchdog = setInterval(() => {
        if (Date.now() - activity > Math.max(90_000, this.config.reconcileMs * 3)) stream.abort();
      }, 30_000);
      try {
        for await (const event of this.gateway.client.event.subscribe({
          types: ["session.step.ended", "session.step.failed", "session.execution.succeeded", "session.execution.failed",
            "session.execution.interrupted", "permission.asked", "permission.replied"],
          signal: AbortSignal.any([signal, stream.signal]), onActivity: () => { activity = Date.now(); },
        })) {
          if (signal.aborted) break;
          attempts = 0;
          this.event(event);
        }
      } catch { if (!signal.aborted) this.notice("OpenCode event stream disconnected; reconnecting and reconciling saved state"); }
      finally { clearInterval(watchdog); stream.abort(); }
      if (signal.aborted) return;
      this.nextFullScan = 0;
      this.kick();
      try { await delay(Math.min(30_000, 1_000 * 2 ** Math.min(attempts++, 5)), undefined, { signal }); }
      catch { return; }
    }
  }

  stop(): Promise<void> {
    if (this.stopTask) return this.stopTask;
    this.phase = "stopped";
    this.controller.abort();
    if (this.timer) clearTimeout(this.timer);
    this.stopTask = (async () => {
      try { await this.slack.stop(); }
      finally {
        await Promise.allSettled([this.work, this.eventTask].filter((task): task is Promise<void> => task !== undefined));
        this.store.close();
      }
    })();
    return this.stopTask;
  }
}

import { createHash, randomBytes, randomUUID } from "node:crypto";
import { chmodSync, closeSync, mkdirSync, openSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { BridgeConfig } from "./config.js";
import { permitted, record, timestamp, type Inbound, type Origin } from "./ingress.js";

export const hash = (value: unknown): string => createHash("sha256").update(JSON.stringify(value) ?? "undefined").digest("hex");
export const originKey = (origin: Origin): string => hash([origin.teamID, origin.channelID, origin.threadTS]);

export interface Binding extends Origin {
  key: string;
  directory: string;
  sessionID: string;
  scanCursor: string | null;
  established: number;
  quarantined: number;
}
export interface InputRow {
  id: string;
  bindingKey: string;
  messageID: string;
  inbound: Inbound;
  attempts: number;
}
export interface Outbound {
  id: string;
  bindingKey: string;
  text: string;
  permissionNonce: string | null;
  status: "pending" | "uncertain" | "sent" | "cancelled";
  held: number;
  slackTS: string | null;
  attempts: number;
  lookupCursor: string | null;
}
export interface PermissionGate {
  nonce: string;
  bindingKey: string;
  requestID: string;
  fingerprint: string;
  expires: number;
  decision: "once" | "reject" | null;
  status: "ready" | "pending" | "done";
  attempts: number;
}

// One owner per state file, released by the OS on crash; no stale PID/lease takeover race.
export class State {
  readonly bridgeID: string;
  private db!: DatabaseSync;
  private lock: DatabaseSync;
  private closed = false;

  constructor(readonly config: BridgeConfig) {
    mkdirSync(dirname(config.stateFile), { recursive: true, mode: 0o700 });
    for (const file of [config.stateFile, `${config.stateFile}.lock.sqlite`]) {
      closeSync(openSync(file, "a", 0o600));
      if (process.platform !== "win32") chmodSync(file, 0o600);
    }
    this.lock = new DatabaseSync(`${config.stateFile}.lock.sqlite`);
    try {
      this.lock.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE; CREATE TABLE IF NOT EXISTS owner (id INTEGER);");
    } catch {
      this.lock.close();
      throw new Error("Bridge state is already owned by another process");
    }
    try {
      this.db = new DatabaseSync(config.stateFile);
      this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS bindings (
          key TEXT PRIMARY KEY, teamID TEXT NOT NULL, channelID TEXT NOT NULL, threadTS TEXT NOT NULL,
          userID TEXT NOT NULL, directory TEXT NOT NULL, sessionID TEXT NOT NULL UNIQUE, scanCursor TEXT, established INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS inputs (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, bindingKey TEXT NOT NULL REFERENCES bindings(key),
          messageID TEXT NOT NULL UNIQUE, messageKey TEXT NOT NULL UNIQUE, inbound TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS outputs (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, bindingKey TEXT NOT NULL REFERENCES bindings(key),
          text TEXT NOT NULL, permissionNonce TEXT, status TEXT NOT NULL DEFAULT 'pending', slackTS TEXT,
          attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, error TEXT, lookupCursor TEXT);
        CREATE TABLE IF NOT EXISTS observed (bindingKey TEXT NOT NULL REFERENCES bindings(key), messageID TEXT NOT NULL,
          PRIMARY KEY(bindingKey, messageID));
        CREATE TABLE IF NOT EXISTS gates (
          nonce TEXT PRIMARY KEY, bindingKey TEXT NOT NULL REFERENCES bindings(key), requestID TEXT NOT NULL,
          fingerprint TEXT NOT NULL, expires INTEGER NOT NULL, decision TEXT, status TEXT NOT NULL DEFAULT 'ready',
          attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, UNIQUE(bindingKey, requestID));`);
      for (const [table, column, definition] of [["bindings", "established", "INTEGER NOT NULL DEFAULT 0"], ["outputs", "lookupCursor", "TEXT"],
        ["bindings", "quarantined", "INTEGER NOT NULL DEFAULT 0"], ["outputs", "held", "INTEGER NOT NULL DEFAULT 0"]]) {
        const fields = this.db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
        if (!fields.some(field => field.name === column)) this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
      }
      // Older holds erased the send outcome. Preserve that ambiguity rather than automatically reposting.
      this.db.exec("UPDATE outputs SET status='uncertain',held=1,error='Legacy routing hold; delivery outcome unknown' WHERE status='held'");
      const identity = hash([config.teamID, config.appID, config.serverURL]);
      const existing = this.db.prepare("SELECT value FROM settings WHERE key='identity'").get() as { value: string } | undefined;
      if (existing && existing.value !== identity) throw new Error("State file belongs to another Slack app, workspace or OpenCode server");
      this.db.prepare("INSERT OR IGNORE INTO settings VALUES ('identity', ?)").run(identity);
      this.db.prepare("INSERT OR IGNORE INTO settings VALUES ('bridgeID', ?)").run(randomUUID());
      this.bridgeID = (this.db.prepare("SELECT value FROM settings WHERE key='bridgeID'").get() as { value: string }).value;
    } catch (error) {
      this.db!?.close();
      this.lock.close();
      throw error;
    }
  }

  private transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try { const value = fn(); this.db.exec("COMMIT"); return value; }
    catch (error) { this.db.exec("ROLLBACK"); throw error; }
  }

  accept(input: Inbound): "accepted" | "duplicate" | "refused" {
    if (!permitted(input, this.config, input.directory)) return "refused";
    return this.transaction(() => {
      const key = originKey(input);
      const binding = this.binding(key);
      if (binding && (binding.quarantined || binding.userID !== input.userID || binding.directory !== input.directory)) return "refused";
      const id = hash([input.teamID, input.eventID]);
      const messageKey = hash([input.teamID, input.channelID, input.messageTS]);
      const previous = this.db.prepare("SELECT inbound FROM inputs WHERE id=? OR messageKey=?").get(id, messageKey) as { inbound: string } | undefined;
      if (previous) {
        const saved = JSON.parse(previous.inbound) as Inbound;
        const signature = (value: Inbound) => hash([value.teamID, value.channelID, value.threadTS, value.userID,
          value.messageTS, value.directory, value.text, value.delivery]);
        return signature(saved) === signature(input) ? "duplicate" : "refused";
      }
      this.db.prepare("INSERT OR IGNORE INTO bindings (key,teamID,channelID,threadTS,userID,directory,sessionID) VALUES (?,?,?,?,?,?,?)")
        .run(key, input.teamID, input.channelID, input.threadTS, input.userID, input.directory, `ses_${randomUUID().replaceAll("-", "")}`);
      this.db.prepare("INSERT INTO inputs (id,bindingKey,messageID,messageKey,inbound) VALUES (?,?,?,?,?)")
        .run(id, key, `msg_${randomUUID().replaceAll("-", "")}`, messageKey, JSON.stringify(input));
      return "accepted";
    });
  }

  binding(key: string): Binding | undefined { return this.db.prepare("SELECT * FROM bindings WHERE key=?").get(key) as unknown as Binding | undefined; }
  bySession(sessionID: string): Binding | undefined { return this.db.prepare("SELECT * FROM bindings WHERE sessionID=?").get(sessionID) as unknown as Binding | undefined; }
  establish(key: string): void { this.db.prepare("UPDATE bindings SET established=1 WHERE key=?").run(key); }
  quarantine(key: string): boolean {
    return this.transaction(() => {
      const changed = this.db.prepare("UPDATE bindings SET quarantined=1 WHERE key=? AND quarantined=0").run(key).changes;
      this.db.prepare("UPDATE outputs SET held=1,error='Session history is quarantined' WHERE bindingKey=? AND status IN ('pending','uncertain')").run(key);
      this.db.prepare("UPDATE inputs SET status='failed',error='Session history is quarantined' WHERE bindingKey=? AND status='pending'").run(key);
      this.db.prepare("UPDATE gates SET status='done' WHERE bindingKey=?").run(key);
      return changed !== 0;
    });
  }
  releaseHolds(key: string): void {
    this.db.prepare(`UPDATE outputs SET held=0,due=0,error=NULL WHERE bindingKey=? AND held=1 AND status IN ('pending','uncertain')
      AND EXISTS (SELECT 1 FROM bindings WHERE key=? AND quarantined=0)`).run(key, key);
  }
  botIdentity(botID: string, userID: string): void {
    const value = hash([botID, userID]);
    const old = this.db.prepare("SELECT value FROM settings WHERE key='bot'").get() as { value: string } | undefined;
    if (old && old.value !== value) throw new Error("State belongs to a different Slack bot identity");
    this.db.prepare("INSERT OR IGNORE INTO settings VALUES ('bot', ?)").run(value);
  }
  reservePost(channelID: string, now: number): boolean {
    const key = `post:${channelID}`;
    const row = this.db.prepare("SELECT value FROM settings WHERE key=?").get(key) as { value: string } | undefined;
    if (row && Number(row.value) > now) return false;
    this.db.prepare("INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(key, String(now + 1_100));
    return true;
  }
  bindings(): Binding[] { return this.db.prepare("SELECT * FROM bindings ORDER BY rowid").all() as unknown as Binding[]; }
  inputs(now: number): InputRow[] {
    const rows = this.db.prepare(`SELECT * FROM inputs i WHERE status='pending' AND due<=? AND NOT EXISTS
      (SELECT 1 FROM inputs earlier WHERE earlier.bindingKey=i.bindingKey AND earlier.status='pending' AND earlier.seq<i.seq)
      ORDER BY seq LIMIT 20`).all(now) as unknown as (Omit<InputRow, "inbound"> & { inbound: string })[];
    return rows.map(row => ({ ...row, inbound: JSON.parse(row.inbound) as Inbound }));
  }
  inputDone(id: string, status = "delivered"): void { this.db.prepare("UPDATE inputs SET status=? WHERE id=?").run(status, id); }
  inputDelivered(row: InputRow): void {
    this.transaction(() => {
      this.inputDone(row.id);
      this.enqueue(`accepted:${row.id}`, row.bindingKey, `Request ${row.inbound.delivery === "steer" ? "sent for steering" : "queued"} in OpenCode. Session: ${this.binding(row.bindingKey)!.sessionID}`);
    });
  }
  inputFailed(row: InputRow): void {
    this.transaction(() => {
      this.inputDone(row.id, "failed");
      this.enqueue(`failed:${row.id}`, row.bindingKey, "Request was not delivered: the saved OpenCode session or its routing no longer matches. Check the bridge locally.");
    });
  }
  hasPendingInputs(): boolean { return !!this.db.prepare("SELECT 1 FROM inputs WHERE status='pending' LIMIT 1").get(); }
  nextDue(): number | undefined {
    const row = this.db.prepare(`SELECT MIN(due) AS due FROM (
      SELECT due FROM inputs i WHERE status='pending' AND NOT EXISTS
        (SELECT 1 FROM inputs p WHERE p.bindingKey=i.bindingKey AND p.status='pending' AND p.seq<i.seq)
      UNION ALL SELECT due FROM gates WHERE status='pending'
      UNION ALL SELECT due FROM outputs o WHERE status IN ('pending','uncertain') AND held=0 AND NOT EXISTS
        (SELECT 1 FROM outputs p WHERE p.bindingKey=o.bindingKey AND p.seq<o.seq AND p.status IN ('pending','uncertain'))
    )`).get() as { due: number | null };
    return row.due ?? undefined;
  }
  inputCount(): number { return (this.db.prepare("SELECT COUNT(*) AS n FROM inputs").get() as { n: number }).n; }
  counts(): Record<string, number> {
    const count = (query: string) => (this.db.prepare(query).get() as { n: number }).n;
    return { bindings: count("SELECT COUNT(*) AS n FROM bindings"), inputs: this.inputCount(),
      pendingInputs: count("SELECT COUNT(*) AS n FROM inputs WHERE status='pending'"),
      pendingOutputs: count("SELECT COUNT(*) AS n FROM outputs WHERE status IN ('pending','uncertain')") };
  }
  inputRetry(id: string, now: number, attempts: number): void {
    this.db.prepare("UPDATE inputs SET attempts=attempts+1,due=?,error='OpenCode request failed' WHERE id=?").run(now + backoff(attempts), id);
  }
  cursor(key: string, cursor: string | null): void { this.db.prepare("UPDATE bindings SET scanCursor=? WHERE key=?").run(cursor, key); }
  observe(key: string, messageID: string, texts: string[]): void {
    this.transaction(() => {
      if (this.db.prepare("SELECT 1 FROM observed WHERE bindingKey=? AND messageID=?").get(key, messageID)) return;
      texts.forEach((text, index) => this.enqueue(`${key}:${messageID}:${index}`, key, text));
      this.db.prepare("INSERT INTO observed VALUES (?,?)").run(key, messageID);
    });
  }
  enqueue(id: string, key: string, text: string, nonce: string | null = null): void {
    this.db.prepare("INSERT OR IGNORE INTO outputs (id,bindingKey,text,permissionNonce,held) VALUES (?,?,?,?,?)")
      .run(id, key, text, nonce, this.binding(key)?.quarantined ? 1 : 0);
  }
  outputs(now: number): Outbound[] {
    return this.db.prepare(`SELECT * FROM outputs o WHERE status IN ('pending','uncertain') AND held=0 AND due<=? AND NOT EXISTS
      (SELECT 1 FROM outputs p WHERE p.bindingKey=o.bindingKey AND p.seq<o.seq AND p.status IN ('pending','uncertain'))
      ORDER BY seq LIMIT 20`).all(now) as unknown as Outbound[];
  }
  sent(id: string, ts: string): void { this.db.prepare("UPDATE outputs SET status='sent',slackTS=?,held=0,error=NULL WHERE id=?").run(ts, id); }
  output(id: string): Outbound | undefined { return this.db.prepare("SELECT * FROM outputs WHERE id=?").get(id) as unknown as Outbound | undefined; }
  uncertain(id: string, due = 0): void { this.db.prepare("UPDATE outputs SET status='uncertain',attempts=attempts+1,due=? WHERE id=?").run(due, id); }
  outputRetry(row: Outbound, now: number, wait?: number): void {
    this.db.prepare("UPDATE outputs SET status='pending',attempts=attempts+1,due=?,error='Slack rejected request' WHERE id=?")
      .run(now + Math.max(wait ?? 0, backoff(row.attempts)), row.id);
  }
  hold(id: string): void { this.db.prepare("UPDATE outputs SET held=1,error='Routing or identity changed' WHERE id=? AND status IN ('pending','uncertain')").run(id); }
  cancelOutput(id: string): void { this.db.prepare("UPDATE outputs SET status='cancelled',held=0 WHERE id=? AND status='pending'").run(id); }
  deferOutput(id: string, now: number): void { this.db.prepare("UPDATE outputs SET due=? WHERE id=?").run(now + 60_000, id); }
  outputDue(id: string, due: number): void { this.db.prepare("UPDATE outputs SET due=? WHERE id=?").run(due, id); }
  lookupCursor(id: string, cursor: string | null): void { this.db.prepare("UPDATE outputs SET lookupCursor=? WHERE id=?").run(cursor, id); }
  confirmReceipt(id: string, origin: { teamID: string; channelID: string; threadTS: string }, ts: string): boolean {
    const row = this.output(id);
    if (!row || row.status !== "uncertain" || !timestamp(ts)) return false;
    const binding = this.binding(row.bindingKey)!;
    if (binding.teamID !== origin.teamID || binding.channelID !== origin.channelID || binding.threadTS !== origin.threadTS) return false;
    this.sent(row.id, ts);
    return true;
  }
  pendingOutbox(): Outbound[] { return this.db.prepare("SELECT * FROM outputs WHERE status IN ('pending','uncertain') ORDER BY seq").all() as unknown as Outbound[]; }
  resolveOutbox(id: string, ts: string): void {
    if (!timestamp(ts)) throw new Error("Invalid Slack message timestamp");
    this.db.prepare("UPDATE outputs SET status='sent',slackTS=?,held=0,error=NULL WHERE id=? AND status='uncertain'").run(ts, id);
  }
  retryOutbox(id: string): void {
    const row = this.output(id);
    if (!row || row.status !== "uncertain" || this.binding(row.bindingKey)?.quarantined) throw new Error("Select an uncertain delivery from an unquarantined route");
    this.db.prepare("UPDATE outputs SET status='pending',due=0 WHERE id=? AND status='uncertain'").run(id);
  }
  discardOutbox(id: string): void {
    this.transaction(() => {
      const row = this.output(id);
      if (!row || !row.held || !["pending", "uncertain"].includes(row.status)) throw new Error("Select a held delivery from outbox");
      this.db.prepare("UPDATE outputs SET status='cancelled',held=0,error='Discarded locally' WHERE id=?").run(id);
      if (row.permissionNonce) this.gateDone(row.permissionNonce);
    });
  }
  gate(key: string, requestID: string, fingerprint: string, text: string, now: number): void {
    this.transaction(() => {
      const old = this.db.prepare("SELECT * FROM gates WHERE bindingKey=? AND requestID=?").get(key, requestID) as unknown as PermissionGate | undefined;
      if (old && old.fingerprint === fingerprint && old.expires > now) return;
      if (old) {
        this.db.prepare("UPDATE outputs SET status='cancelled',held=0 WHERE permissionNonce=? AND status='pending'").run(old.nonce);
        this.db.prepare("DELETE FROM gates WHERE nonce=?").run(old.nonce);
      }
      const nonce = randomBytes(24).toString("hex");
      const result = this.db.prepare("INSERT OR IGNORE INTO gates (nonce,bindingKey,requestID,fingerprint,expires) VALUES (?,?,?,?,?)")
        .run(nonce, key, requestID, fingerprint, now + this.config.permissionTTLms);
      if (result.changes) this.enqueue(`permission:${key}:${requestID}:${nonce}`, key, text, nonce);
    });
  }
  getGate(nonce: string): PermissionGate | undefined { return this.db.prepare("SELECT * FROM gates WHERE nonce=?").get(nonce) as unknown as PermissionGate | undefined; }
  acceptInteraction(body: unknown, now: number): boolean {
    const b = record(body), team = record(b?.team), user = record(b?.user), channel = record(b?.channel), container = record(b?.container), message = record(b?.message);
    if (!b || b.type !== "block_actions" || b.api_app_id !== this.config.appID || team?.id !== this.config.teamID ||
        !Array.isArray(b.actions) || b.actions.length !== 1 || container?.type !== "message" || container.is_ephemeral === true) return false;
    const action = record(b.actions[0]);
    if (!action || action.type !== "button" || action.block_id !== "opencode_slack_permission" ||
        typeof action.value !== "string" || !["opencode_slack_allow", "opencode_slack_reject"].includes(String(action.action_id))) return false;
    return this.transaction(() => {
      const gate = this.getGate(action.value as string);
      if (!gate || gate.status !== "ready" || gate.expires <= now) return false;
      const binding = this.binding(gate.bindingKey)!;
      const out = this.db.prepare("SELECT slackTS FROM outputs WHERE permissionNonce=? AND status='sent'").get(gate.nonce) as { slackTS: string } | undefined;
      if (!out || binding.quarantined || !permitted(binding, this.config, binding.directory) || user?.id !== binding.userID ||
          (user.team_id !== undefined && user.team_id !== binding.teamID) || channel?.id !== binding.channelID ||
          container.channel_id !== binding.channelID || container.message_ts !== out.slackTS || message?.ts !== out.slackTS ||
          message.thread_ts !== binding.threadTS) return false;
      this.db.prepare("UPDATE gates SET decision=?,status='pending' WHERE nonce=? AND status='ready'")
        .run(action.action_id === "opencode_slack_allow" ? "once" : "reject", gate.nonce);
      return true;
    });
  }
  gates(now: number): PermissionGate[] { return this.db.prepare("SELECT * FROM gates WHERE status='pending' AND due<=? ORDER BY rowid LIMIT 20").all(now) as unknown as PermissionGate[]; }
  gateDone(nonce: string): void { this.db.prepare("UPDATE gates SET status='done' WHERE nonce=?").run(nonce); }
  gateOutcome(gate: PermissionGate, text: string): void {
    this.transaction(() => {
      this.gateDone(gate.nonce);
      this.enqueue(`decision:${gate.nonce}`, gate.bindingKey, text);
    });
  }
  gateRetry(gate: PermissionGate, now: number): void { this.db.prepare("UPDATE gates SET attempts=attempts+1,due=? WHERE nonce=?").run(now + backoff(gate.attempts), gate.nonce); }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
    this.lock.close();
  }
}

export const backoff = (attempts: number): number => Math.min(60_000, 1_000 * 2 ** Math.min(attempts, 6));

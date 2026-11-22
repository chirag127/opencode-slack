import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { State, originKey } from "../src/state.js";
import { parseInbound } from "../src/ingress.js";
import { configuration, envelope, interaction, temporary } from "./fixtures.js";

test("retries and concurrent same-thread ingress preserve one owner and stable prompt IDs across restart", async t => {
  const temp = temporary();
  const config = configuration(temp.directory);
  let state = new State(config); t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!;
  const results = await Promise.all(Array.from({ length: 8 }, () => Promise.resolve().then(() => state.accept(input))));
  assert.equal(results.filter(value => value === "accepted").length, 1);
  assert.equal(results.filter(value => value === "duplicate").length, 7);
  const binding = state.binding(originKey(input))!, prompt = state.inputs(0)[0]!;
  assert.equal(binding.userID, "UALICE");
  assert.equal(state.accept({ ...input, eventID: "Ev2", userID: "UBOB", messageTS: "1795301000.000003" }), "refused");
  assert.equal(state.accept({ ...input, text: "changed replay" }), "refused");
  assert.equal(state.accept({ ...input, eventID: "Ev2" }), "duplicate");
  state.close(); state = new State(config);
  assert.equal(state.accept(input), "duplicate");
  assert.equal(state.binding(binding.key)?.sessionID, binding.sessionID);
  assert.equal(state.inputs(0)[0]?.messageID, prompt.messageID);
  assert.equal(state.inputCount(), 1);
});

test("file ownership fails closed and a clean close releases the process lock", t => {
  const temp = temporary();
  const config = configuration(temp.directory), state = new State(config);
  t.after(() => { state.close(); temp.remove(); });
  assert.throws(() => new State(config), /already owned/);
  state.close();
  assert.throws(() => new State({ ...config, teamID: "TOTHER" }), /another Slack/);
  const reopened = new State(config); reopened.close();
});

test("a failed first prompt blocks later same-thread prompts but not other threads", t => {
  const temp = temporary();
  const config = configuration(temp.directory), state = new State(config); t.after(() => { state.close(); temp.remove(); });
  const first = parseInbound(envelope(), config, "UBRIDGE")!;
  state.accept(first);
  state.accept({ ...first, eventID: "Ev2", messageTS: "1795301000.000003", text: "second" });
  state.accept({ ...first, eventID: "Ev3", threadTS: "1795301000.000004", messageTS: "1795301000.000005", text: "other" });
  const ready = state.inputs(0); assert.equal(ready.length, 2);
  state.inputRetry(ready[0]!.id, 100, 0);
  assert.deepEqual(state.inputs(101).map(row => row.inbound.text), ["other"]);
  state.inputDone(ready[0]!.id);
  assert.deepEqual(state.inputs(101).map(row => row.inbound.text), ["second", "other"]);
});

test("permission buttons require the owning user, workspace, channel, thread, message, nonce and one-shot consent", t => {
  const temp = temporary();
  const config = configuration(temp.directory), state = new State(config); t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!; state.accept(input);
  const key = originKey(input);
  state.gate(key, "perm_one", "fingerprint", "Run npm test?", 100);
  const output = state.outputs(100)[0]!, nonce = output.permissionNonce!, ts = "1795302000.000001";
  assert.equal(state.acceptInteraction(interaction(nonce, ts), 101), false, "unsent message cannot grant consent");
  state.sent(output.id, ts);
  for (const override of [
    { user: { id: "UBOB", team_id: "TDEMO" } }, { team: { id: "TOTHER" } }, { api_app_id: "AOTHER" },
    { channel: { id: "CSECOND" } }, { message: { ts, thread_ts: "1795301000.000999" } },
    { container: { type: "message", channel_id: "CPROJECT", message_ts: "1795302000.000002" } },
    { actions: [{ type: "button", block_id: "opencode_slack_permission", action_id: "opencode_slack_allow", value: "not-the-nonce" }] },
  ]) assert.equal(state.acceptInteraction(interaction(nonce, ts, override), 101), false);
  assert.equal(state.acceptInteraction(interaction(nonce, ts), 101), true);
  assert.equal(state.acceptInteraction(interaction(nonce, ts), 102), false);
  assert.equal(state.gates(102)[0]?.decision, "once");
});

test("expired or changed permission grants invalidate old buttons without blocking new outbox work", t => {
  const temp = temporary();
  const config = configuration(temp.directory), state = new State(config); t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!; state.accept(input);
  const key = originKey(input);
  state.gate(key, "perm_one", "old", "Old operation", 100);
  const old = state.outputs(100)[0]!;
  state.gate(key, "perm_one", "new", "New operation", 101);
  assert.equal(state.output(old.id)?.status, "cancelled");
  const next = state.outputs(102)[0]!;
  assert.notEqual(next.permissionNonce, old.permissionNonce);
  state.sent(next.id, "1795302000.000002");
  assert.equal(state.acceptInteraction(interaction(next.permissionNonce!, "1795302000.000002"), 101 + config.permissionTTLms), false);
});

test("unconfirmed posts persist, stop ordered reposting, and require a receipt from the exact thread", t => {
  const temp = temporary();
  const config = configuration(temp.directory); let state = new State(config); t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!; state.accept(input);
  const key = originKey(input);
  state.observe(key, "msg_answer", ["first", "second"]);
  state.observe(key, "msg_answer", ["must not duplicate"]);
  const first = state.outputs(0)[0]!; state.uncertain(first.id);
  state.close(); state = new State(config);
  assert.equal(state.outputs(0).length, 1);
  assert.equal(state.outputs(0)[0]?.status, "uncertain");
  assert.equal(state.confirmReceipt(first.id, { ...input, channelID: "CSECOND" }, "1795302000.000001"), false);
  assert.equal(state.confirmReceipt(first.id, input, "1795302000.000001"), true);
  assert.equal(state.outputs(0)[0]?.text, "second");
  assert.equal(state.pendingOutbox().length, 1);
});

test("a routing hold preserves send uncertainty and accepts a confirmed receipt after restart", t => {
  const temp = temporary(), config = configuration(temp.directory);
  let state = new State(config);
  t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!;
  state.accept(input);
  const key = originKey(input);
  state.enqueue("held-send", key, "Already possibly sent");
  state.enqueue("held-next", key, "Later output");
  state.uncertain("held-send");
  state.hold("held-send");
  state.close(); state = new State(config);
  assert.equal(state.output("held-send")?.status, "uncertain");
  assert.equal(state.confirmReceipt("held-send", input, "1795302000.000001"), true);
  assert.equal(state.outputs(Date.now())[0]?.id, "held-next");
});

test("legacy routing holds migrate without assuming the interrupted post was unsent", t => {
  const temp = temporary(), config = configuration(temp.directory);
  let state = new State(config);
  t.after(() => { state.close(); temp.remove(); });
  const input = parseInbound(envelope(), config, "UBRIDGE")!;
  state.accept(input);
  const key = originKey(input);
  state.enqueue("legacy-held", key, "An older process lost this send outcome");
  state.enqueue("legacy-next", key, "Later ordered result");
  state.uncertain("legacy-held");
  state.close();
  const legacy = new DatabaseSync(config.stateFile);
  try {
    legacy.exec("ALTER TABLE outputs DROP COLUMN held; ALTER TABLE bindings DROP COLUMN quarantined;");
    legacy.prepare("UPDATE outputs SET status='held' WHERE id=?").run("legacy-held");
  } finally { legacy.close(); }
  state = new State(config);
  assert.equal(state.output("legacy-held")?.status, "uncertain");
  assert.equal(state.output("legacy-held")?.held, 1);
  assert.deepEqual(state.outputs(Date.now()), []);
  state.releaseHolds(key);
  assert.equal(state.outputs(Date.now())[0]?.status, "uncertain", "route restoration cannot authorize a blind repost");
  state.resolveOutbox("legacy-held", "1795302000.000001");
  assert.equal(state.outputs(Date.now())[0]?.id, "legacy-next");
  assert.equal(state.binding(key)?.sessionID.startsWith("ses_"), true);
});

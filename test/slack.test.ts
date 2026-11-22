import assert from "node:assert/strict";
import { test } from "node:test";
import { SlackTransport } from "../src/slack.js";
import { State } from "../src/state.js";
import { parseInbound } from "../src/ingress.js";
import { configuration, credentials, envelope, rootTS, SlackFixture, temporary, changedUntil } from "./fixtures.js";

test("real Bolt startup authenticates the configured workspace and app before opening Socket Mode", async t => {
  const temp = temporary(), slack = new SlackFixture();
  await slack.start();
  t.after(async () => { await slack.stop(); temp.remove(); });
  const config = configuration(temp.directory);
  for (const mismatch of ["team", "app"] as const) {
    slack.authTeam = mismatch === "team" ? "TFOREIGN" : "TDEMO";
    slack.botApp = mismatch === "app" ? "AFOREIGN" : "ADEMO";
    const transport = new SlackTransport(config, credentials, { admit() { assert.fail("unauthenticated admission"); }, connected() {}, notice() {} }, { fetch: slack.fetch });
    try { await assert.rejects(transport.start(), /does not match/); }
    finally { await transport.stop(); }
  }
  assert.equal(slack.connectionCount, 0);
  assert.equal(slack.requests.some(request => request.method === "apps.connections.open"), false);
});

test("history recovery only confirms the bot's exact delivery and paginates without reposting", async t => {
  const temp = temporary(), slack = new SlackFixture();
  await slack.start();
  const config = configuration(temp.directory), state = new State(config);
  const historyToken = "xoxp-synthetic-readonly-history";
  const transport = new SlackTransport(config, { ...credentials, historyToken }, { admit() {}, connected() {}, notice() {} }, { fetch: slack.fetch });
  t.after(async () => { await transport.stop(); state.close(); await slack.stop(); temp.remove(); });
  await transport.start();
  state.accept(parseInbound(envelope(), config, "UBRIDGE")!);
  const binding = state.bindings()[0]!;
  state.enqueue("first", binding.key, "Earlier reply");
  state.enqueue("target", binding.key, "Target reply");
  const target = state.output("target")!;
  await transport.post(binding, state.output("first")!, state.bridgeID);
  const ts = await transport.post(binding, target, state.bridgeID);
  slack.historyPageSize = 1;
  const first = await transport.recover(binding, target, state.bridgeID);
  assert.deepEqual(first, { cursor: "1" });
  const found = await transport.recover(binding, { ...target, lookupCursor: first.cursor }, state.bridgeID);
  assert.deepEqual(found, { ts, cursor: null });
  const wrongBot = slack.posts[1]!;
  wrongBot.bot_id = "BOTHER";
  assert.deepEqual(await transport.recover(binding, { ...target, lookupCursor: "1" }, state.bridgeID), { cursor: null });
  assert.equal(slack.posts.length, 2);
  const reads = slack.requests.filter(request => request.method === "conversations.replies");
  assert.equal(reads.length, 3);
  assert.ok(reads.every(request => request.body.channel === "CPROJECT" && request.body.ts === rootTS));
  assert.ok(reads.every(request => request.token === `Bearer ${historyToken}`));
  assert.ok(slack.requests.filter(request => request.method === "chat.postMessage").every(request => request.token === `Bearer ${credentials.botToken}`));
});

test("output is bounded plain Slack content with no mention expansion or thread broadcast", async t => {
  const temp = temporary(), slack = new SlackFixture();
  await slack.start();
  const config = configuration(temp.directory), state = new State(config);
  const transport = new SlackTransport(config, credentials, { admit() {}, connected() {}, notice() {} }, { fetch: slack.fetch });
  t.after(async () => { await transport.stop(); state.close(); await slack.stop(); temp.remove(); });
  await transport.start();
  state.accept(parseInbound(envelope(), config, "UBRIDGE")!);
  const binding = state.bindings()[0]!;
  state.enqueue("plain", binding.key, "<!channel> <@UALICE> " + "x".repeat(6_000));
  await transport.post(binding, state.output("plain")!, state.bridgeID);
  const request = slack.requests.find(request => request.method === "chat.postMessage")!.body;
  assert.equal(request.channel, "CPROJECT"); assert.equal(request.thread_ts, rootTS);
  assert.equal(String(request.reply_broadcast), "false");
  assert.equal(String(request.mrkdwn), "false");
  assert.equal(String(request.link_names), "false");
  assert.equal(request.parse, "none");
  assert.equal(String(request.unfurl_links), "false");
  assert.equal(request.blocks.length, 3);
  assert.ok(request.blocks.every((block: { text: { type: string; text: string } }) => block.text.type === "plain_text" && block.text.text.length <= 2_800));
  assert.ok(request.text.startsWith("&lt;!channel&gt; &lt;@UALICE&gt;"));
});

test("failed durable admission is not acknowledged, and a retried envelope is stored only once", async t => {
  const temp = temporary(), slack = new SlackFixture();
  await slack.start();
  const config = configuration(temp.directory), state = new State(config);
  let unavailable = true;
  const transport = new SlackTransport(config, credentials, {
    admit(body) {
      if (unavailable) throw new Error("synthetic storage failure");
      const input = parseInbound(body, config, "UBRIDGE");
      if (input) state.accept(input);
    }, connected() {}, notice() {},
  }, { fetch: slack.fetch });
  t.after(async () => { await transport.stop(); state.close(); await slack.stop(); temp.remove(); });
  await transport.start();
  await assert.rejects(slack.send(envelope()), /ack timed out/);
  assert.equal(state.inputCount(), 0);
  unavailable = false;
  await slack.send(envelope());
  await slack.send(envelope());
  assert.equal(state.inputCount(), 1);
});

test("stopping cancels an in-flight Socket Mode reconnect without reopening a connection", async t => {
  const temp = temporary(), slack = new SlackFixture();
  await slack.start();
  const transport = new SlackTransport(configuration(temp.directory), credentials,
    { admit() {}, connected() {}, notice() {} }, { fetch: slack.fetch });
  let release!: () => void;
  t.after(async () => { release?.(); await transport.stop(); await slack.stop(); temp.remove(); });
  await transport.start();
  slack.blockOpen = new Promise<void>(resolve => { release = resolve; });
  slack.refresh();
  await changedUntil(slack, () => slack.requests.filter(row => row.method === "apps.connections.open").length, count => count === 2);
  await Promise.all([transport.stop(), transport.stop()]);
  release();
  assert.equal(slack.connectionCount, 1);
  assert.equal(slack.activeSockets, 0);
  const opens = slack.transportResults.filter(row => row.method === "/api/apps.connections.open");
  assert.equal(opens.length, 2);
  assert.equal(opens[1]?.error, "AbortError");
});

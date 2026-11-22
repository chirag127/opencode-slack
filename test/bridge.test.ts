import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { test, type TestContext } from "node:test";
import { SlackBridge } from "../src/bridge.js";
import { State } from "../src/state.js";
import type { BridgeConfig, Credentials } from "../src/config.js";
import { configuration, credentials, envelope, interaction, rootTS, temporary, changedUntil,
  OpenCodeFixture, SlackFixture, type SlackPost } from "./fixtures.js";

async function fixture(t: TestContext, credentialOverrides: Partial<Credentials> = {}) {
  const temp = temporary(), code = new OpenCodeFixture(), slack = new SlackFixture();
  const notices: string[] = [], changes = new EventEmitter();
  await code.start();
  await slack.start();
  const config = configuration(temp.directory, code.url);
  const options = { slack: { fetch: slack.fetch }, notice: (text: string) => { notices.push(text); changes.emit("change"); } };
  const auth = { ...credentials, ...credentialOverrides };
  const result = { code, slack, config, notices, changes, bridge: new SlackBridge(config, auth, options),
    async restart(overrides: Partial<BridgeConfig> = {}) {
      await result.bridge.stop();
      result.config = { ...result.config, ...overrides };
      result.bridge = new SlackBridge(result.config, auth, options);
      await result.bridge.start();
    } };
  t.after(async () => {
    t.diagnostic(JSON.stringify({ bridge: result.bridge.status(), notices,
      prompts: [...code.prompts.values()].map(row => ({ text: row.text, delivery: row.delivery })),
      posts: slack.posts.map(post => ({ channel: post.channel, thread: post.thread_ts, text: post.text.slice(0, 100) })),
      replies: code.replies.length, streams: code.streamCount, connections: slack.connectionCount,
      transport: slack.transportResults, fixtureErrors: slack.fixtureErrors }));
    try { await result.bridge.stop(); }
    finally { await Promise.all([slack.stop(), code.stop()]); temp.remove(); }
  });
  await Promise.all([result.bridge.start(), result.bridge.start()]);
  return result;
}

function permissionButton(post: SlackPost): string | undefined {
  return post.blocks.find(block => block.type === "actions")?.elements?.[0]?.value as string | undefined;
}

test("real Socket Mode retries and concurrent mentions retain one session owner and exact output threads", async t => {
  const f = await fixture(t);
  await Promise.all(Array.from({ length: 6 }, () => f.slack.send(envelope())));
  const [first] = await f.code.waitPrompts(1);
  assert.ok(first);
  assert.equal(f.slack.connectionCount, 1);
  assert.equal(f.bridge.status().counts.inputs, 1);

  f.slack.raw("not-json");
  const denied = [
    envelope({ user: "UBOB", ts: "1795301000.000003" }, { event_id: "EvOtherOwner" }),
    envelope({ user: "UOUTSIDER" }, { event_id: "EvWrongUser" }),
    envelope({ channel: "CFORBIDDEN" }, { event_id: "EvWrongChannel" }),
    envelope({ bot_id: "BBRIDGE" }, { event_id: "EvBot" }),
    envelope({}, { team_id: "TOTHER", event_id: "EvWrongTeam" }),
    envelope({}, { api_app_id: "AOTHER", event_id: "EvWrongApp" }),
  ];
  await Promise.all(denied.map(body => f.slack.send(body)));
  await Promise.all([
    f.slack.send(envelope({ ts: "1795301000.000004", text: "<@UBRIDGE> !steer Check this first" }, { event_id: "EvSteer" })),
    f.slack.send(envelope({ ts: "1795301000.000005", text: "<@UBRIDGE> /permissions allow always" }, { event_id: "EvText" })),
  ]);
  const prompts = await f.code.waitPrompts(3);
  assert.equal(new Set(prompts.map(row => row.sessionID)).size, 1);
  assert.equal(prompts.find(row => row.text === "Check this first")?.delivery, "steer");
  assert.equal(prompts.find(row => row.text === "/permissions allow always")?.delivery, "queue");
  assert.equal(f.code.replies.length, 0);
  assert.equal(f.bridge.status().counts.inputs, 3);

  await f.slack.send(envelope({ user: "UBOB", channel: "CSECOND", thread_ts: "1795301000.000100",
    ts: "1795301000.000101", text: "<@UBRIDGE> Separate task" }, { event_id: "EvSecond" }));
  const second = (await f.code.waitPrompts(4)).find(row => row.text === "Separate task")!;
  assert.notEqual(second.sessionID, first.sessionID);
  f.code.finish(first.sessionID, "Only the original thread receives this result.");
  f.code.finish(second.sessionID, "Only the second thread receives this result.");
  const original = await f.slack.waitPost(post => post.text === "Only the original thread receives this result.");
  const other = await f.slack.waitPost(post => post.text === "Only the second thread receives this result.");
  assert.equal(original.channel, "CPROJECT"); assert.equal(original.thread_ts, rootTS);
  assert.equal(other.channel, "CSECOND"); assert.equal(other.thread_ts, "1795301000.000100");
  assert.ok(f.code.creates.every(row => row.permissions === undefined));
  assert.equal(f.code.requests.some(row => /\/(command|synthetic|environment)$/.test(row.path)), false);
});

test("permission interactions acknowledge before remote work and only the bound user can approve the exact pending request", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  f.code.ask(prompt!.sessionID);
  const card = await f.slack.waitPost(post => permissionButton(post) !== undefined);
  await f.bridge.reconcile();
  const nonce = permissionButton(card)!;
  for (const override of [
    { user: { id: "UBOB", team_id: "TDEMO" } }, { team: { id: "TOTHER" } },
    { channel: { id: "CSECOND" } }, { message: { ts: card.ts, thread_ts: "1795301000.000999" } },
    { api_app_id: "AOTHER" },
  ]) await f.slack.send(interaction(nonce, card.ts, override), "interactive");
  assert.equal(f.code.replies.length, 0);

  let release!: () => void;
  f.code.blockedPermissionRead = new Promise<void>(resolve => { release = resolve; });
  t.after(() => release());
  await f.slack.send(interaction(nonce, card.ts), "interactive");
  assert.equal(f.code.replies.length, 0, "acknowledgment does not wait for or imply permission approval");
  release();
  await changedUntil(f.code, () => f.code.replies, rows => rows.length === 1);
  assert.deepEqual(f.code.replies[0], { sessionID: prompt!.sessionID, requestID: "perm_example", decision: "once" });
  await f.slack.send(interaction(nonce, card.ts), "interactive");
  f.code.ask(prompt!.sessionID, "perm_new");
  await f.slack.send(interaction(nonce, card.ts), "interactive");
  await f.bridge.reconcile();
  assert.equal(f.code.replies.length, 1, "a replay cannot approve the next permission");
});

test("a changed permission fingerprint and a lost approval response cannot approve different work", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  const request = f.code.ask(prompt!.sessionID);
  const card = await f.slack.waitPost(post => permissionButton(post) !== undefined);
  await f.bridge.reconcile();
  let release!: () => void;
  f.code.blockedPermissionRead = new Promise<void>(resolve => { release = resolve; });
  t.after(() => release());
  await f.slack.send(interaction(permissionButton(card)!, card.ts), "interactive");
  // Change the request after durable admission, before the remote permission check can finish.
  request.resources = ["git push"];
  release();
  const changedNotice = await f.slack.waitPost(post => post.text.includes("no longer pending"));
  assert.equal(f.code.replies.length, 0);
  const replacement = await f.slack.waitPost(post => permissionButton(post) !== undefined && post.ts !== card.ts);
  assert.ok(replacement.text.includes("git push"));
  await f.bridge.reconcile();
  f.code.replyLostOnce = true;
  await f.slack.send(interaction(permissionButton(replacement)!, replacement.ts), "interactive");
  await changedUntil(f.code, () => f.code.replies, rows => rows.length === 1);
  await f.slack.waitPost(post => post.text.includes("no longer pending") && post.ts !== changedNotice.ts);
  await f.bridge.reconcile();
  assert.equal(f.code.replies.length, 1);
});

test("restart and event-stream reconnect reconcile saved results without claiming event replay or duplicating prompts", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  // The fixture observes server receipt before the HTTP client confirms the write.
  // Let this graceful-restart case finish the send; interrupted sends have separate coverage.
  await f.bridge.reconcile();
  await f.bridge.stop();
  f.code.finish(prompt!.sessionID, "Finished while the bridge was stopped.", "msg_offline", false);
  await f.restart();
  await f.slack.waitPost(post => post.text === "Finished while the bridge was stopped.");
  await f.slack.send(envelope());
  assert.equal(f.bridge.status().counts.inputs, 1);
  const streamCount = f.code.streamCount;
  f.code.dropStreams();
  f.code.finish(prompt!.sessionID, "Missed event recovered from the session.", "msg_gap", false);
  await changedUntil(f.code, () => f.code.streamCount, count => count > streamCount);
  await f.slack.waitPost(post => post.text === "Missed event recovered from the session.");
  const connections = f.slack.connectionCount;
  f.slack.refresh();
  await f.slack.waitConnections(connections + 1);
  await f.slack.send(envelope({ ts: "1795301000.000003", text: "<@UBRIDGE> Next task" }, { event_id: "EvNext" }));
  await f.code.waitPrompts(2);
  assert.equal(f.code.creates.length, 1);
  assert.equal(f.slack.posts.filter(post => post.text === "Finished while the bridge was stopped.").length, 1);
  assert.equal(f.slack.posts.filter(post => post.text === "Missed event recovered from the session.").length, 1);
});

test("offline OpenCode admission is durable before ack and drains once connectivity returns", async t => {
  const f = await fixture(t);
  f.code.offline = true;
  await f.slack.send(envelope());
  assert.equal(f.bridge.status().counts.pendingInputs, 1);
  assert.equal(f.code.prompts.size, 0);
  f.code.offline = false;
  await f.bridge.reconcile();
  await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  assert.equal(f.bridge.status().counts.pendingInputs, 0);
});

for (const reason of ["refresh", "unexpected close"] as const) {
  test(`Socket Mode ${reason} survives failed connection opens and keeps prompt deduplication`, async t => {
    const f = await fixture(t);
    await f.slack.send(envelope());
    const [first] = await f.code.waitPrompts(1);
    await f.slack.waitPost(post => post.text.includes("Session:"));
    await f.bridge.reconcile();
    const connections = f.slack.connectionCount;
    f.slack.openFaults = ["transport", "http"];
    if (reason === "refresh") f.slack.refresh(); else f.slack.dropConnections();
    await f.slack.waitConnections(connections + 1);
    assert.equal(f.bridge.status().phase, "running");
    assert.equal(f.slack.activeSockets, 1);
    await f.slack.send(envelope());
    await f.slack.send(envelope({ ts: "1795301000.000003", text: "<@UBRIDGE> After reconnect" }, { event_id: "EvReconnect" }));
    const prompts = await f.code.waitPrompts(2);
    assert.equal(prompts.length, 2);
    assert.ok(prompts.every(prompt => prompt.sessionID === first!.sessionID));
    assert.equal(f.code.creates.length, 1);
    f.code.finish(first!.sessionID, "Result after connection-open failures.");
    const result = await f.slack.waitPost(post => post.text === "Result after connection-open failures.");
    assert.equal(result.thread_ts, rootTS);
    assert.equal(f.slack.posts.filter(post => post.text === result.text).length, 1);
    const opens = f.slack.transportResults.filter(row => row.method === "/api/apps.connections.open");
    assert.ok(opens.some(row => row.error));
    assert.ok(opens.some(row => row.status === 503));
  });
}

test("an uncertain Slack write stays in the outbox until its exact bot receipt arrives", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  f.slack.postFault = "after";
  f.code.finish(prompt!.sessionID, "Stored once despite the lost HTTP response.");
  const lost = await f.slack.waitPost(post => post.text === "Stored once despite the lost HTTP response.");
  await changedUntil(f.changes, () => f.notices, values => values.some(value => value.includes("unconfirmed")));
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text === lost.text).length, 1);
  await f.slack.receipt({ ...lost, thread_ts: "1795301000.000999" });
  assert.equal(f.bridge.status().counts.pendingOutputs, 1);
  await f.slack.receipt(lost);
  assert.equal(f.bridge.status().counts.pendingOutputs, 0);
  assert.equal(f.code.prompts.size, 1, "bot delivery receipts never become prompts");
});

test("shutdown cancels an in-flight transport request, releases state and does not create a second connection", async t => {
  const temp = temporary();
  const config = configuration(temp.directory), changes = new EventEmitter();
  let called = false;
  const bridge = new SlackBridge(config, credentials, { slack: { fetch: async (_url, init) => {
    called = true; changes.emit("change");
    return new Promise<never>((_resolve, reject) => {
      const abort = () => reject(new Error("synthetic cancellation"));
      if (init?.signal?.aborted) abort(); else init?.signal?.addEventListener("abort", abort, { once: true });
    });
  } } });
  t.after(async () => { await bridge.stop(); temp.remove(); });
  const start = bridge.start();
  const rejected = assert.rejects(start);
  await changedUntil(changes, () => called, Boolean);
  await Promise.all([bridge.stop(), bridge.stop(), rejected]);
  assert.equal(bridge.status().phase, "stopped");
  const state = new State(config); state.close();
});

test("an interrupted confirmed-at-Slack write survives restart and an exact receipt unblocks later output", async t => {
  const f = await fixture(t);
  let release!: () => void;
  f.slack.blockPostResponse = new Promise<void>(resolve => { release = resolve; });
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  const accepted = await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.stop();
  release();
  f.slack.blockPostResponse = undefined;
  f.code.finish(prompt!.sessionID, "Recovered after interrupted confirmation.", "msg_interrupted", false);
  await f.restart();
  assert.equal(f.slack.posts.length, 1, "uncertain writes must not be repeated automatically");
  await f.slack.receipt(accepted);
  await f.slack.waitPost(post => post.text === "Recovered after interrupted confirmation.");
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text.includes("Request queued")).length, 1);
  assert.equal(f.bridge.status().counts.pendingOutputs, 0);
  assert.equal(f.code.prompts.size, 1);
});

test("an explicit Slack rate rejection retries once while unknown post outcomes remain uncertain", async t => {
  const f = await fixture(t);
  f.slack.postFault = "rate";
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  assert.equal(f.slack.requests.filter(row => row.method === "chat.postMessage").length, 2);
  assert.equal(f.slack.posts.length, 1);
  f.slack.postFault = "internal";
  f.code.finish(prompt!.sessionID, "Ambiguous server-side outcome.");
  const ambiguous = await f.slack.waitPost(post => post.text === "Ambiguous server-side outcome.");
  await changedUntil(f.changes, () => f.notices, values => values.some(value => value.includes("unconfirmed (platform")));
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text === ambiguous.text).length, 1);
  assert.equal(f.bridge.status().counts.pendingOutputs, 1);
  await f.slack.receipt(ambiguous);
  assert.equal(f.bridge.status().counts.pendingOutputs, 0);
});

test("output pagination and invalid-cursor recovery do not skip a streaming message or duplicate final output", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  const messages = f.code.messages.get(prompt!.sessionID)!;
  for (let index = 0; index < 104; index++) messages.push({ id: `msg_context_${index}`, type: "system", text: "fixture context", time: { created: index } });
  const streaming = { id: "msg_streaming", type: "assistant" as const, agent: "build", model: { providerID: "synthetic", id: "no-inference" },
    time: { created: Date.now() } as { created: number; completed?: number }, content: [{ type: "text" as const, text: "Not final" }] };
  messages.push(streaming);
  f.code.invalidCursorOnce = true;
  f.code.emitEvent(prompt!.sessionID);
  await changedUntil(f.code, () => f.code.messagePages, rows => rows.some(row => row.cursor === "fixture-page-100"));
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.some(post => post.text === "Not final"), false);
  streaming.time.completed = Date.now();
  streaming.content[0]!.text = "Final streamed result.";
  f.code.emitEvent(prompt!.sessionID);
  await f.slack.waitPost(post => post.text === "Final streamed result.");
  f.code.invalidCursorOnce = true;
  f.code.emitEvent(prompt!.sessionID);
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text === "Final streamed result.").length, 1);
});

test("a short final message page is exhausted before the bridge returns to its next scan", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  f.code.finish(prompt!.sessionID, "Final short-page result.", "msg_short_final");
  await f.slack.waitPost(post => post.text === "Final short-page result.");
  await changedUntil(f.code, () => f.code.messagePages, rows => rows.some(row => row.cursor === "fixture-page-2" && row.count === 0));
  await f.bridge.reconcile();
  assert.equal(f.code.messagePages.some(row => row.status === 400), false);
  assert.equal(f.slack.posts.filter(post => post.text === "Final short-page result.").length, 1);
});

test("moving a mapped session holds its output instead of leaking another directory into Slack", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  f.code.sessions.get(prompt!.sessionID)!.location.directory = "/workspace/other-private-project";
  f.code.finish(prompt!.sessionID, "Must never leave the mismatched route.");
  await changedUntil(f.changes, () => f.notices, values => values.some(value => value.includes("no longer matches")));
  assert.equal(f.slack.posts.some(post => post.text.includes("Must never")), false);
  assert.equal(f.code.replies.length, 0);
});

test("an unfinished historical assistant cannot hide later pages, and its eventual completion is still delivered", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  const messages = f.code.messages.get(prompt!.sessionID)!;
  const interrupted = { id: "msg_old_stream", type: "assistant" as const, agent: "build",
    model: { providerID: "synthetic", id: "no-inference" }, time: { created: Date.now() } as { created: number; completed?: number },
    content: [{ type: "text" as const, text: "Unfinished old output" }] };
  messages.push(interrupted);
  for (let i = 0; i < 105; i++) messages.push({ id: `msg_gap_${i}`, type: "system", text: "historical context", time: { created: i } });
  f.code.finish(prompt!.sessionID, "Result after the unfinished page.", "msg_later_page");
  await f.slack.waitPost(post => post.text === "Result after the unfinished page.");
  assert.equal(f.slack.posts.some(post => post.text === "Unfinished old output"), false);
  interrupted.time.completed = Date.now();
  interrupted.content[0]!.text = "The older message finished later.";
  f.code.emitEvent(prompt!.sessionID);
  await f.slack.waitPost(post => post.text === "The older message finished later.");
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text === "Result after the unfinished page.").length, 1);
  assert.equal(f.slack.posts.filter(post => post.text === "The older message finished later.").length, 1);
});

test("a permission card exposes operation metadata and source before accepting a decision", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  f.code.ask(prompt!.sessionID, "perm_metadata", { tool: "deploy", input: { environment: "staging", command: "release candidate-42" } });
  const card = await f.slack.waitPost(post => permissionButton(post) !== undefined);
  assert.ok(card.text.includes("release candidate-42"), "the human must see the actual tool arguments");
  assert.ok(card.text.includes("staging"));
  assert.ok(card.text.includes("tool_example"), "the source tool identity must be visible");
  assert.equal(f.code.replies.length, 0);
});

test("offline move-away and move-back history is durably quarantined even behind a saved page cursor", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  await f.bridge.stop();
  const sessionID = prompt!.sessionID, messages = f.code.messages.get(sessionID)!;
  for (let index = 0; index < 98; index++) messages.push({ id: `msg_route_context_${index}`, type: "system", text: "Context", time: { created: index } });
  messages.push({ id: "msg_move_away", type: "location-switched", time: { created: Date.now() },
    location: { directory: "/workspace/private" }, previous: { location: { directory: "/workspace/project" } } });
  f.code.finish(sessionID, "FOREIGN-DIRECTORY-RESULT", "msg_foreign_result", false);
  messages.push({ id: "msg_move_back", type: "location-switched", time: { created: Date.now() },
    location: { directory: "/workspace/project" }, previous: { location: { directory: "/workspace/private" } } });
  const state = new State(f.config);
  try { state.cursor(state.bindings()[0]!.key, "fixture-page-100"); }
  finally { state.close(); }
  f.slack.on("change", () => f.changes.emit("change"));
  await f.restart();
  await changedUntil(f.changes, () => f.notices.some(text => text.includes("quarantin")) ||
    f.slack.posts.some(post => post.text === "FOREIGN-DIRECTORY-RESULT"), Boolean);
  assert.equal(f.slack.posts.some(post => post.text === "FOREIGN-DIRECTORY-RESULT"), false);
  assert.ok(f.notices.some(text => text.includes("quarantin")));
  // A later projection no longer exposing the old move must not clear the durable quarantine.
  f.code.messages.set(sessionID, [messages[0]!]);
  await f.restart();
  await f.slack.send(envelope({ ts: "1795301000.000003", text: "<@UBRIDGE> Must not reuse this session" }, { event_id: "EvAfterMove" }));
  await f.bridge.reconcile();
  assert.equal(f.code.prompts.size, 1);
  assert.equal(f.code.replies.length, 0);
});

test("restoring a temporary allowlist removal releases pending output in thread order", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  await f.bridge.stop();
  const state = new State(f.config);
  try {
    const binding = state.bindings()[0]!;
    state.enqueue("held-first", binding.key, "First pending result.");
    state.enqueue("held-second", binding.key, "Second pending result.");
  } finally { state.close(); }
  await f.restart({ users: ["UBOB"] });
  assert.equal(f.slack.posts.some(post => post.text === "First pending result."), false);
  await f.restart({ users: ["UALICE", "UBOB"] });
  await f.slack.waitPost(post => post.text === "Second pending result.");
  await f.bridge.reconcile();
  assert.deepEqual(f.slack.posts.filter(post => post.text.includes("pending result.")).map(post => post.text),
    ["First pending result.", "Second pending result."]);
  assert.equal(f.bridge.status().counts.pendingOutputs, 0);
});

test("restoring a temporary allowlist removal preserves uncertainty until an exact receipt", async t => {
  const f = await fixture(t);
  await f.slack.send(envelope());
  const [prompt] = await f.code.waitPrompts(1);
  await f.slack.waitPost(post => post.text.includes("Session:"));
  await f.bridge.reconcile();
  f.slack.postFault = "after";
  f.code.finish(prompt!.sessionID, "Accepted before the route hold.", "msg_before_hold");
  const lost = await f.slack.waitPost(post => post.text === "Accepted before the route hold.");
  await changedUntil(f.changes, () => f.notices, rows => rows.some(text => text.includes("unconfirmed")));
  await f.bridge.stop();
  const state = new State(f.config);
  try {
    const id = lost.metadata.event_payload.deliveryID, row = state.output(id)!;
    state.outputDue(id, 0);
    state.enqueue("after-held-uncertain", row.bindingKey, "After the confirmed held post.");
  } finally { state.close(); }
  await f.restart({ users: ["UBOB"] });
  await f.restart({ users: ["UALICE", "UBOB"] });
  assert.equal(f.slack.posts.filter(post => post.text === lost.text).length, 1);
  assert.equal(f.slack.posts.some(post => post.text === "After the confirmed held post."), false);
  await f.slack.receipt(lost);
  await f.slack.waitPost(post => post.text === "After the confirmed held post.");
  await f.bridge.reconcile();
  assert.equal(f.slack.posts.filter(post => post.text === lost.text).length, 1);
  assert.equal(f.bridge.status().counts.pendingOutputs, 0);
});

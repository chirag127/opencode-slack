import assert from "node:assert/strict";
import { test } from "node:test";
import { OpenCodeGateway, permissionFingerprint, RoutingError } from "../src/opencode.js";
import { State } from "../src/state.js";
import { parseInbound } from "../src/ingress.js";
import { OpenCodeFixture, configuration, credentials, envelope, temporary } from "./fixtures.js";

test("the published V2 client recovers lost create and prompt responses with fixed IDs, preserving native permissions", async t => {
  const temp = temporary();
  const service = new OpenCodeFixture(); await service.start();
  const config = configuration(temp.directory, service.url), state = new State(config);
  t.after(async () => { state.close(); await service.stop(); temp.remove(); });
  const gateway = new OpenCodeGateway(config, credentials), signal = new AbortController().signal;
  await gateway.check(signal);
  state.accept(parseInbound(envelope(), config, "UBRIDGE")!);
  const binding = state.bindings()[0]!, input = state.inputs(0)[0]!;
  service.createLostReply = true;
  await assert.rejects(gateway.session(binding, state.bridgeID, signal, true));
  await gateway.session(binding, state.bridgeID, signal, true);
  assert.equal(service.creates.length, 1);
  assert.equal(service.creates[0]?.permissions, undefined);
  assert.equal(service.creates[0]?.agent, undefined);
  service.promptFault = "after";
  await assert.rejects(gateway.prompt(binding, input, signal));
  await gateway.prompt(binding, input, signal);
  await gateway.prompt(binding, input, signal);
  assert.equal(service.prompts.size, 1);
  assert.equal(service.prompts.get(input.messageID)?.delivery, "queue");
  assert.equal(service.prompts.get(input.messageID)?.text, "Fix the parser");
  assert.equal(service.prompts.get(input.messageID)?.metadata.slack.userID, "UALICE");
  assert.equal(service.requests.some(req => /\/command$|\/synthetic$|\/environment$/.test(req.path)), false);
});

test("routing mismatch cannot attach an existing session or reuse a deleted established session", async t => {
  const temp = temporary();
  const service = new OpenCodeFixture(); await service.start();
  const config = configuration(temp.directory, service.url), state = new State(config);
  t.after(async () => { state.close(); await service.stop(); temp.remove(); });
  const gateway = new OpenCodeGateway(config, credentials), signal = new AbortController().signal;
  state.accept(parseInbound(envelope(), config, "UBRIDGE")!);
  const binding = state.bindings()[0]!;
  await gateway.session(binding, state.bridgeID, signal, true);
  service.sessions.get(binding.sessionID)!.location = { directory: "/workspace/secret" };
  await assert.rejects(gateway.session(binding, state.bridgeID, signal), RoutingError);
  service.sessions.delete(binding.sessionID);
  await assert.rejects(gateway.session({ ...binding, established: 1 }, state.bridgeID, signal, true), RoutingError);
  assert.equal(service.creates.length, 1);
});

test("permission fingerprints retain tool provenance and ignore object-key order", () => {
  const request = { id: "p1", sessionID: "ses_example", action: "shell", resources: ["npm test"], metadata: { b: 2, a: 1 } };
  assert.equal(permissionFingerprint(request), permissionFingerprint({ ...request, metadata: { a: 1, b: 2 } }));
  assert.notEqual(permissionFingerprint(request), permissionFingerprint({ ...request, resources: ["git push"] }));
  assert.notEqual(permissionFingerprint(request), permissionFingerprint({ ...request, sessionID: "ses_other" }));
});

test("message continuation omits order and follows a short nonempty page to exhaustion", async t => {
  const temp = temporary(), service = new OpenCodeFixture();
  await service.start();
  const config = configuration(temp.directory, service.url), state = new State(config);
  t.after(async () => { state.close(); await service.stop(); temp.remove(); });
  const gateway = new OpenCodeGateway(config, credentials), signal = new AbortController().signal;
  state.accept(parseInbound(envelope(), config, "UBRIDGE")!);
  const binding = state.bindings()[0]!;
  await gateway.session(binding, state.bridgeID, signal, true);
  service.messages.get(binding.sessionID)!.push(
    { id: "msg_first", type: "system", text: "First", time: { created: 1 } },
    { id: "msg_last", type: "system", text: "Last", time: { created: 2 } },
  );
  const first = await gateway.messages(binding, signal);
  assert.deepEqual(first.data.map(message => message.id), ["msg_first", "msg_last"]);
  assert.ok(first.cursor.next, "a short page still supplies the server's continuation cursor");
  const end = await gateway.messages({ ...binding, scanCursor: first.cursor.next }, signal);
  assert.deepEqual(end.data, []);
  assert.equal(end.cursor.next, undefined);
  assert.deepEqual(service.messagePages.filter(row => row.type === null).map(({ order, cursor, status, count }) => ({ order, cursor, status, count })), [
    { order: "asc", cursor: null, status: 200, count: 2 },
    { order: null, cursor: first.cursor.next, status: 200, count: 0 },
  ]);
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { parseInbound, type AccessPolicy } from "../src/ingress.js";

const policy: AccessPolicy = {
  teamID: "TDEMO",
  appID: "ADEMO",
  users: ["UALICE", "UBOB"],
  channels: [{ channelID: "CPROJECT", directory: "/workspace/project" }],
  maxInputChars: 8_000,
};

function envelope(overrides: Record<string, unknown> = {}, event: Record<string, unknown> = {}) {
  return {
    type: "event_callback",
    api_app_id: "ADEMO",
    team_id: "TDEMO",
    event_id: "Ev1",
    event: {
      type: "app_mention",
      user: "UALICE",
      channel: "CPROJECT",
      ts: "1795301000.000002",
      thread_ts: "1795301000.000001",
      text: "<@UBRIDGE> Fix the parser",
      ...event,
    },
    ...overrides,
  };
}

test("an authorized mention preserves its exact thread and queues ordinary text", () => {
  assert.deepEqual(parseInbound(envelope(), policy, "UBRIDGE"), {
    teamID: "TDEMO",
    channelID: "CPROJECT",
    threadTS: "1795301000.000001",
    userID: "UALICE",
    messageTS: "1795301000.000002",
    eventID: "Ev1",
    directory: "/workspace/project",
    text: "Fix the parser",
    delivery: "queue",
  });
});

test("only an explicit leading !steer opts into steering; commands remain prompt text", () => {
  assert.equal(parseInbound(envelope({}, { text: "<@UBRIDGE> !steer Check this first" }), policy, "UBRIDGE")?.delivery, "steer");
  const text = "/permissions approve always; !shell echo hi; mention !steer later";
  const parsed = parseInbound(envelope({}, { text: `<@UBRIDGE> ${text}` }), policy, "UBRIDGE");
  assert.equal(parsed?.text, text);
  assert.equal(parsed?.delivery, "queue");
});

test("foreign teams, users, channels, apps and bot-shaped events cannot enter the bridge", () => {
  const bodies = [
    envelope({ team_id: "TOTHER" }),
    envelope({ api_app_id: "AOTHER" }),
    envelope({}, { user: "UOTHER" }),
    envelope({}, { channel: "COTHER" }),
    envelope({}, { user: "UBRIDGE" }),
    envelope({}, { bot_id: "BOTHER" }),
    envelope({}, { app_id: "AOTHER" }),
    envelope({}, { subtype: "bot_message" }),
    envelope({}, { subtype: "message_changed" }),
    envelope({}, { hidden: true }),
    envelope({}, { user_team: "TOTHER" }),
    envelope({}, { team: "TOTHER" }),
    envelope({}, { type: "message" }),
    envelope({}, { text: "Someone mentioned <@UBRIDGE> incidentally" }),
    envelope({}, { text: "<@UBRIDGE> !steer" }),
    envelope({}, { text: "<@UBRIDGE> " + "x".repeat(8_001) }),
    envelope({}, { thread_ts: "../other" }),
    envelope({ event_id: undefined }),
    { type: "block_actions", user: { id: "UALICE" }, actions: [{ value: "approve" }] },
    null,
  ];
  for (const body of bodies) assert.equal(parseInbound(body, policy, "UBRIDGE"), undefined);
});

test("a root mention uses its own timestamp and decodes only Slack text entities", () => {
  const parsed = parseInbound(envelope({}, { thread_ts: undefined, text: "<@UBRIDGE> x &lt; y &amp; z &gt; w" }), policy, "UBRIDGE");
  assert.equal(parsed?.threadTS, "1795301000.000002");
  assert.equal(parsed?.text, "x < y & z > w");
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { loadCredentials, parseConfig, sameDirectory, serverDirectory } from "../src/config.js";
import { bounded, chunks, escapeSlack, redactor } from "../src/text.js";
import { credentials } from "./fixtures.js";

const config = { teamID: "TDEMO", appID: "ADEMO", users: ["UALICE"],
  channels: [{ channelID: "CPROJECT", directory: "/work/app" }],
  serverURL: "http://127.0.0.1:4096", stateFile: ".state/bridge.sqlite" };

test("configuration requires explicit identity and routes and refuses embedded credentials or insecure remote transport", () => {
  assert.equal(parseConfig(config).channels[0]?.directory, "/work/app");
  for (const override of [
    { users: [] }, { users: ["*"] }, { channels: [] }, { teamID: "*" },
    { channels: [{ channelID: "CPROJECT", directory: "relative" }] },
    { serverURL: "http://remote.example" }, { serverURL: "https://user:secret@example.com" },
    { serverURL: "https://example.com/?token=secret" }, { botToken: "must-not-be-config" },
    { maxOutputChars: 100_000 }, { permissionTTLms: 0 },
  ]) assert.throws(() => parseConfig({ ...config, ...override }));
});

test("paths use server rather than bridge-host syntax and do not equate distinct POSIX paths", () => {
  assert.equal(serverDirectory("C:\\work\\app\\"), "C:/work/app");
  assert.equal(serverDirectory("C:\\"), "C:/");
  assert.equal(sameDirectory("C:\\Work\\App", "c:/work/app"), true);
  assert.equal(sameDirectory("/work/App", "/work/app"), false);
  assert.throws(() => serverDirectory("C:relative"));
});

test("credential validation and redaction do not include secrets in errors or retained text", () => {
  assert.throws(() => loadCredentials({}), /SLACK_BOT_TOKEN/);
  assert.throws(() => loadCredentials({ SLACK_BOT_TOKEN: "bad\nvalue" }), /SLACK_BOT_TOKEN/);
  const redact = redactor({ ...credentials, historyToken: "xoxp-synthetic-history" });
  const result = redact([credentials.botToken, credentials.appToken, credentials.serverPassword,
    "xoxp-synthetic-history", "xoxb-another-token"].join(" "));
  assert.equal(result, "[redacted] [redacted] [redacted] [redacted] [redacted]");
});

test("bounded plain-text output cannot split surrogate pairs or invoke Slack mentions", () => {
  const text = "a".repeat(2799) + "😀" + "b".repeat(2800);
  const parts = chunks(text);
  assert.equal(parts.join(""), text);
  assert.ok(parts.every(part => part.length <= 2800 && !/[\uD800-\uDBFF]$/.test(part)));
  assert.ok(bounded(text, 256).length <= 256);
  assert.ok(bounded(text, 256).includes("Truncated"));
  assert.equal(escapeSlack("<!channel> <@UALICE> &"), "&lt;!channel&gt; &lt;@UALICE&gt; &amp;");
  assert.throws(() => chunks("x", 0));
});

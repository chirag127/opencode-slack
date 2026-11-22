#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { SlackBridge } from "./bridge.js";
import { loadCredentials, parseConfig } from "./config.js";
import { State } from "./state.js";

const help = `Usage: opencode-slack <start|status|outbox|resolve|retry|discard> <config.json> [delivery-id] [slack-ts]

start    Connect one Socket Mode bridge; tokens come only from environment variables.
status   Inspect durable work counts while the bridge is stopped.
outbox   Inspect pending delivery IDs/statuses while stopped; does not print message text.
resolve  Mark an uncertain delivery confirmed after checking its timestamp in Slack.
retry    Explicitly retry an uncertain delivery after checking Slack; may duplicate a sent message.
discard  Abandon a held delivery locally; does not delete any message already posted to Slack.

Required environment for start: SLACK_BOT_TOKEN, SLACK_APP_TOKEN, OPENCODE_SERVER_PASSWORD.
Optional: OPENCODE_SERVER_USERNAME (default opencode), SLACK_HISTORY_TOKEN (read-only thread recovery).
Permissions are answered only through identity-bound buttons, never message text.`;

async function main(): Promise<void> {
  const [command, file, deliveryID, ts] = process.argv.slice(2);
  if (!command || command === "--help" || command === "-h") { console.log(help); return; }
  if (!file || !["start", "status", "outbox", "resolve", "retry", "discard"].includes(command)) throw new Error("Invalid command; use --help");
  const configPath = resolve(file);
  const config = parseConfig(JSON.parse(await readFile(configPath, "utf8")), dirname(configPath));
  if (command !== "start") {
    const state = new State(config);
    try {
      if (command === "status") console.log(JSON.stringify(state.counts(), null, 2));
      else if (command === "outbox") console.log(JSON.stringify(state.pendingOutbox().map(row => ({
        id: row.id, status: row.status, held: !!row.held, attempts: row.attempts, bindingKey: row.bindingKey,
        channelID: state.binding(row.bindingKey)?.channelID, threadTS: state.binding(row.bindingKey)?.threadTS,
        quarantined: !!state.binding(row.bindingKey)?.quarantined,
      })), null, 2));
      else {
        if (!deliveryID) throw new Error("Select a delivery from outbox");
        if (command === "discard") state.discardOutbox(deliveryID);
        else if (state.output(deliveryID)?.status !== "uncertain") throw new Error("Select an uncertain delivery from outbox");
        else if (command === "resolve") {
          if (!ts) throw new Error("A confirmed Slack timestamp is required");
          state.resolveOutbox(deliveryID, ts);
        } else state.retryOutbox(deliveryID);
        console.log("Outbox resolution saved.");
      }
    } finally { state.close(); }
    return;
  }
  const bridge = new SlackBridge(config, loadCredentials(process.env), { notice: text => console.error(text) });
  let shutdown: Promise<void> | undefined;
  const stop = () => {
    shutdown ??= bridge.stop().catch(() => { console.error("Bridge shutdown failed"); process.exitCode = 1; });
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await bridge.start();
    console.error("Slack bridge started. Press Ctrl+C to stop.");
  } catch {
    stop();
    await shutdown;
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
    throw new Error("Bridge could not start; check configuration, token scopes and service availability");
  }
}

main().catch(() => { console.error("opencode-slack failed. Check configuration and service access; use --help for usage."); process.exitCode = 1; });

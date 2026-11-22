# opencode-slack

Self-hosted Slack bridge for OpenCode V2 using Slack Bolt. One standalone Socket
Mode process maps allowlisted Slack threads to durable OpenCode sessions, queues
requests, reconciles output after reconnects, and offers identity-bound **Allow
once** / **Reject** permission buttons.

Requires Node.js 24+, Slack Bolt 5.1.0, and an authenticated OpenCode V2 server.
The client contract is pinned to `@opencode/client` 2.3.2. The package provides an
ESM library (`SlackBridge`, `parseConfig`, `loadCredentials`) and the
`opencode-slack` CLI.

## Setup

1. Create a Slack app using [`examples/slack-app-manifest.json`](examples/slack-app-manifest.json).
   Enable Socket Mode, create an app-level token with `connections:write`, install
   the app in your workspace, and invite the bot to each configured channel.
2. Edit [`examples/bridge.json`](examples/bridge.json): set your workspace/app IDs,
   explicit user allowlist, channel IDs, and each channel's absolute **server-side**
   project directory. Set the authenticated OpenCode server URL (HTTPS, or HTTP on
   loopback). Relative `stateFile` paths resolve from the JSON file's directory.
3. Supply credentials through the process or service-manager environment:

   | Variable | Purpose |
   | --- | --- |
   | `SLACK_BOT_TOKEN` | Installed app's bot token |
   | `SLACK_APP_TOKEN` | App-level Socket Mode token |
   | `OPENCODE_SERVER_PASSWORD` | OpenCode server password |
   | `OPENCODE_SERVER_USERNAME` | Optional; defaults to `opencode` |
   | `SLACK_HISTORY_TOKEN` | Optional same-workspace user token for read-only thread recovery |

   The optional history token needs read access to the configured threads
   (`channels:history` for public channels, `groups:history` for private channels).
   It is never used to post messages. Do not put credentials in the JSON file.
4. Build and start from a source checkout:

   ```sh
   npm ci --ignore-scripts
   npm run check
   node dist/cli.js start ./examples/bridge.json
   ```

   With an installed package, use `opencode-slack start <config.json>`.

Only one process may own a state file. Protect its directory with your operating
system's account permissions. Socket Mode needs outbound connectivity and no
public HTTP listener. Stop with Ctrl+C; shutdown leaves remote OpenCode sessions
intact.

## Using the bridge

Begin each request with `@your-bot`. The first allowlisted user in a thread owns
its session; later requests from that user queue in the same session. Use
`@your-bot !steer <request>` for explicit steering. Output returns to the original
channel and thread, where other channel members can read it.

Permission cards show the operation, source, and metadata. Only the thread owner
can use **Allow once** or **Reject**; ordinary message text never grants permission.
Each action has a one-use nonce and the pending request is re-fetched and compared
before reply. This is stale-action validation: OpenCode 2.3.2 replies are addressed
by session/request ID, without an atomic server-side fingerprint condition.
Oversized requests must be reviewed in OpenCode. Existing OpenCode permission
rules continue to apply.

## Recovery and CLI

The bridge stores admission, thread/session mappings, and output in local SQLite.
Reconnects reconcile saved messages; OpenCode's live event stream is not replayed.
Unconfirmed Slack writes block later output in their thread until an authenticated
bot receipt, optional history lookup, or explicit local resolution confirms them.

Stop the bridge before using the state-management commands:

```sh
node dist/cli.js status ./examples/bridge.json
node dist/cli.js outbox ./examples/bridge.json
node dist/cli.js resolve ./examples/bridge.json <delivery-id> <confirmed-slack-ts>
node dist/cli.js retry ./examples/bridge.json <delivery-id>
node dist/cli.js discard ./examples/bridge.json <held-delivery-id>
```

`outbox` shows delivery IDs, statuses, holds, and originating threads without
printing message text. Check that thread in Slack before `resolve` or `retry`:
an explicit retry can duplicate a previously accepted post. Temporary route or
allowlist holds are released after revalidation, retaining send uncertainty.
Foreign directory history permanently quarantines a session; use a new Slack
thread. `discard` abandons held output locally without deleting a Slack message
or clearing that quarantine. Run `node dist/cli.js --help` for command usage.

For embedding, import `SlackBridge`, `parseConfig`, and `loadCredentials` from
`opencode-slack`; call `start()` and await `stop()` during shutdown. The bridge is
a standalone connection owner, rather than a per-directory OpenCode plugin.

## Development notes

`npm run check` runs TypeScript checks, synthetic HTTP/WebSocket boundary tests,
and the portable JavaScript build. Live Slack validation requires your own
authorized app/workspace. Maintainer design and delivery records are
**workspace-private** at `C:/ws/docs/okf/opencode`; they are not public package
documentation or required for setup.

<footer class="hire-me">
  <h3>Hire me</h3>
  <p>Chirag Singhal — building free, open APIs and tools.</p>
  <p>
    <a href="mailto:chirag@oriz.in">chirag@oriz.in</a> ·
    <a href="https://github.com/chirag127">GitHub</a> ·
    <a href="https://www.linkedin.com/in/chirag127/">LinkedIn</a> ·
    <a href="https://x.com/singhal127">X</a> ·
    <a href="https://www.youtube.com/@ChiragSinghal127">YouTube</a> ·
    <a href="https://www.instagram.com/chiragsinghal127/">Instagram</a> ·
    <a href="https://www.reddit.com/user/Far_Macaron_9910/">Reddit</a> ·
    <a href="https://buymeacoffee.com/chirag127">Buy me a coffee</a>
  </p>
</footer>

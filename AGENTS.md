# OpenCode Slack bridge

This repository owns one self-hosted Slack Bolt 5.1.0 Socket Mode bridge over the published OpenCode client 2.3.2. Preserve workspace/channel/thread/user identity, durable admission and deduplication, uncertain-send recovery and one-use identity-bound permission buttons.

## Development and verification

- Use Node 24 or later and the pinned dependency lockfile. Run `npm ci --ignore-scripts` and `npm run check` from this repository.
- Tests use synthetic credentials and loopback HTTP/WebSocket fixtures under the real Bolt/OpenCode client boundaries. Do not read actual secrets or connect to a live Slack workspace during implementation/tests.
- Tokens come from environment variables. Keep message bodies and credentials out of diagnostic output.
- Acknowledge only after durable admission. Own reconnect failures and cancellation; do not blindly retry a possibly delivered post.
- Continuation queries use the opaque cursor without `order`. Audit retained location changes before output and quarantine foreign history. Routing holds must preserve send uncertainty.
- Permission replies retain one-use local identity and stale-request validation. The OpenCode 2.3.2 reply API is ID-addressed, not an atomic fingerprint-conditioned operation.
- Verify the built tarball's imports, declarations and CLI with a separate consumer. Source publication alone does not deliver the ignored `dist/` entrypoints. Preserve the MIT license and exclude generated state, consumers and credentials from Git.

## Maintainer workspace

Canonical detailed documentation and continuation state are workspace-private at `C:/ws/docs/okf/opencode`, including `tasks/feature-parity-20261121.md`. Do not copy that knowledge base into this repository. Public setup and recovery commands are self-contained in README.md and examples/.

In the maintainer workspace, this project is registered at `repos/own/public/libraries/opencode-slack`. Follow `C:/ws/AGENTS.md` and its autonomous-coding policy when available. Concurrent workers retain exclusive source scopes; the integration owner alone changes shared configuration, Git integration and canonical notes. Preserve existing worktrees and unrelated work.

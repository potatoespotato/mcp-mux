# mcp-mux

Share one stdio **MCP** or **LSP** server between many clients.

Claude Code (and most other agent hosts) starts a fresh child process per
session for every stdio MCP server and every language server. Run five sessions
and you get five copies of the same index, five `tsserver`s, five
`rust-analyzer`s. mcp-mux keeps **one backend per server (per workspace root)**
and lets every session talk to it through a tiny shim.

```
 session A ─ mcpmux-shim ─┐
 session B ─ mcpmux-shim ─┼─ unix socket ─ mcpmuxd ─ stdio ─ real server
 session C ─ mcpmux-shim ─┘
```

- `mcpmux-shim` is what the host launches. A dumb stdio ⇄ unix socket pipe,
  ~400 KB, no async runtime. It starts the daemon on first use.
- `mcpmuxd` owns the backend and multiplexes all shims onto its single
  stdin/stdout. It speaks the protocol (`--proto mcp|lsp`), so it can rewrite
  ids and keep shared state consistent. About 2–3 MiB RSS.
- `mcpmux` is the CLI: `status` (who is running, how much memory) and `stop`.

Linux only for now: it relies on `/proc`, `flock` and `setsid`.

## Numbers

Measured on one small TypeScript repo, one file open:

| backend                                | first diagnostics | backend RSS |
| -------------------------------------- | ----------------- | ----------- |
| `typescript-language-server` (TS 6)    | 4.7 s             | 1593 MiB    |
| `tsgo --lsp` (TS 7) behind `mcpmuxd`   | 0.15 s            | 54 MiB      |

Every extra session on the same root costs one shim instead of another server.

## Install

```sh
cargo install --git https://github.com/potatoespotato/mcp-mux
```

This puts `mcpmux-shim`, `mcpmuxd` and `mcpmux` into `~/.cargo/bin`. The shim
looks for `mcpmuxd` **next to itself**, so keep the three binaries together.

## Use with MCP servers

Prefix the server command with the shim:

```sh
claude mcp add my-index -- mcpmux-shim --name my-index -- my-mcp-server --flag
```

Options:

| flag            | meaning                                                                 |
| --------------- | ----------------------------------------------------------------------- |
| `--name NAME`   | daemon identity; shims with the same key share one backend               |
| `--cwd-scoped`  | append a hash of the current directory to the key: one backend per root  |
| `--idle SECS`   | stop the daemon after this long without clients (`0`, default: never)   |
| `--proto P`     | `mcp` (default) or `lsp`                                                  |

What the daemon does for MCP:

- the first `initialize` goes to the backend; its result is cached and served
  to every later client; `notifications/initialized` is sent once;
- request ids and `progressToken`s are rewritten per client and mapped back;
- `notifications/cancelled` is translated; a disconnecting client's pending
  requests are cancelled;
- server notifications are broadcast; server requests (sampling, roots,
  elicitation) go to the oldest connected client.

## Use with language servers (Claude Code plugin)

This repo is also a Claude Code plugin marketplace. The `lsp-mux` plugin runs
TypeScript/JavaScript (`tsgo`), Python (`pyright`), Rust (`rust-analyzer`) and
C/C++ (`clangd`) through the multiplexer, one server per workspace root,
stopped after 10 minutes without sessions.

1. Install the servers you want. For TypeScript 7 expose it as `tsgo`:

   ```sh
   npm i --prefix ~/.local/lib/ts7 typescript@7
   ln -s ~/.local/lib/ts7/node_modules/typescript/bin/tsc ~/.local/bin/tsgo
   ```

2. Add the marketplace, install the plugin, and turn off the official plugins
   for the same languages so two servers do not fight over one extension:

   ```sh
   claude plugin marketplace add potatoespotato/mcp-mux
   claude plugin install lsp-mux@mcp-mux
   claude plugin disable typescript-lsp@claude-plugins-official
   ```

If a server binary is missing, the daemon logs `cannot start backend` and the
shim exits at once, so only that language goes without code intelligence. Edit `plugins/lsp-mux/.lsp.json` to add or remove
languages.

### What the daemon does for LSP

LSP has shared server-side state, so the daemon does more than for MCP:

- **Lifecycle.** `initialize` is cached like MCP. `processId` is replaced with
  the daemon's pid, otherwise the server exits together with the first client.
  A client's `shutdown` is answered by the daemon and its `exit` only closes
  that client; the backend keeps running.
- **Documents.** The server holds one copy per URI. The daemon counts which
  clients have a document open, forwards the first `didOpen` and the last
  `didClose`, turns a later client's `didOpen` into a full-text `didChange`,
  and owns the version counter. Clients are told the server wants **full**
  sync, so no client sends a range edit against text another client changed.
- **Diagnostics** go only to clients that have the URI open. The last set is
  cached and replayed to a client that opens the document later.
- **Pull → push bridge.** Some servers (TypeScript 7) only serve pull
  diagnostics (`textDocument/diagnostic`), while Claude Code only accepts
  pushed `publishDiagnostics`. When the server offers pull and a client does
  not, the daemon pulls after every change (and, with `interFileDependencies`,
  re-pulls the other open documents) and publishes the result itself. Stale
  replies are dropped.
- **Server requests.** `client/registerCapability`, `unregisterCapability` and
  `window/workDoneProgress/create` are answered by the daemon (dynamic
  registration is per-client state it cannot fan out). Other server requests,
  like `workspace/configuration`, go to the oldest client; with no client
  attached the daemon answers with an error instead of letting the server hang.
- **Progress / cancellation.** `workDoneToken`, `partialResultToken` and
  `$/cancelRequest` are rewritten per client.

### Limits

- Clients share a server only when they share a workspace root
  (`--cwd-scoped`). Two different roots get two servers.
- Server-wide settings come from the first client's `initialize` and
  `workspace/configuration` answers. Fine for identical clients (several
  Claude Code sessions), questionable for mixed editors.
- Two clients editing the same unsaved document at once get last-writer-wins.
  Agent hosts edit files on disk, so in practice the text is the same.

## Operate

```sh
mcpmux status          # daemons, clients, daemon/backend RSS, command
mcpmux stop fff        # SIGTERM every daemon whose key starts with "fff"
mcpmux stop --all
```

Files:

- sockets, pid and metadata: `$XDG_RUNTIME_DIR/mcpmux/` (fallback
  `/tmp/mcpmux-$UID/mcpmux/`)
- daemon logs: `$XDG_STATE_HOME/mcpmux/<key>.log` (fallback
  `~/.local/state/mcpmux/`); the backend's stderr goes there too.

Unix socket paths are limited to 108 bytes; keep `XDG_RUNTIME_DIR` short.

## Build

```sh
cargo build --release
```

`mcpmux-shim` is dependency-free; `mcpmuxd` uses `tokio` (current-thread
runtime) and `serde_json`. All routing state lives on one task, so the protocol
code has no locks.

## License

MIT

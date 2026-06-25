# wikigame

Find the shortest path between any two Wikipedia articles using bidirectional BFS on the full English Wikipedia link graph.

## How it works

1. **Download** the three Wikimedia SQL dumps (`page`, `pagelinks`, `linktarget`).
2. **Build** a compressed sparse row (CSR) graph from the dumps (~1–3 h, one-time).
3. **Search** for the shortest click-path between any two articles, either from the CLI or a local web UI.

## Prerequisites

- [Rust](https://rustup.rs/) (stable, 2021 edition)
- ~12 GB free disk space for the dumps and processed graph files
- ~4 GB RAM available during `build`; `serve`/`search` keep the 5 GB graph memory-mapped, so they use very little RSS

## Quick start

```sh
# 1. Build the release binary
cargo build --release

# 2. Just run it — opens your browser, shows a setup wizard the first time,
#    downloads + builds the graph with live progress, then starts the search UI.
./target/release/wikigame
```

The first launch opens a setup wizard in the browser:

1. **Download** the three Wikimedia SQL dumps (~10 GB, resumable).
2. **Build** a compressed sparse row (CSR) graph from the dumps (~1–3 h, with
   live progress in the browser).
3. **Cleanup** — raw `.sql.gz` dumps are deleted as they're parsed.
4. **Search** — the 3D UI loads automatically once the graph is ready.

Subsequent launches skip setup (the graph is already on disk) and go straight
to the search UI.

### Subcommands

You can also run individual steps directly:

```sh
./target/release/wikigame download          # download dumps only
./target/release/wikigame build              # parse + build graph only
./target/release/wikigame search "Kevin Bacon" "Philosophy"
./target/release/wikigame serve --port 8080  # start the web server only
```

The web UI shows the shortest path as an interactive 3-D scene. Path nodes are drawn in blue, and a cloud of neighboring articles (up to a configurable limit) is drawn in purple around each path node.

## Commands

| Command | Description |
|---|---|
| *(none)* | Default: open browser → setup wizard if needed → search UI. |
| `download [--downloads <dir>]` | Download the three SQL dumps from Wikimedia. |
| `build [--downloads <dir>] [--data <dir>] [--no-download] [--keep-dumps]` | Parse dumps and write binary graph files. Use `--keep-dumps` to keep the raw `.sql.gz` files after building. |
| `search <from> <to> [-i] [--data <dir>]` | Find the shortest path between two articles. Pass `-i` to keep the graph in memory for repeated queries. |
| `serve [--port <port>] [--data <dir>]` | Start the local web server (default port 8080). |

## API endpoints (used by the web UI)

| Endpoint | Method | Body | Response |
|---|---|---|---|
| `/api/status` | GET | — | `{ "state": "needs_setup" \| "building" \| "ready" \| "error", "message"? }` |
| `/api/setup` | POST | — | Starts download + build in the background. `202` if started, `409` if already running, `200` if already ready. |
| `/api/progress` | GET | — | SSE stream of `{ kind, phase, message, current?, total? }` events. |
| `/search` | POST | `{ "from": "...", "to": "..." }` | `{ "path": [...], "hops": N, "ms": N, "error?": "..." }` (503 unless ready) |
| `/neighbors` | POST | `{ "title": "...", "limit": 30 }` | `{ "title": "...", "total": N, "neighbors": [...], "error?": "..." }` (503 unless ready) |

## Resuming a failed build

If `build` is interrupted during the CSR construction phase, but `data/processed/edges.tmp` and `data/processed/title_index.bin` already exist, re-running `wikigame build` will skip parsing and downloading and resume from the CSR step. This saves re-downloading the large `.sql.gz` dumps.

## Case sensitivity

Article titles must match Wikipedia exactly and are **case-sensitive**. For example:

- ✅ `Banana` → `Adolf Hitler`
- ❌ `banana` → `Adolf Hitler` (lowercase `banana` is not an article title)

Spaces are automatically converted to underscores, so both `"Kevin Bacon"` and `"Kevin_Bacon"` work.

## Data directories

| Path | Contents |
|---|---|
| `data/downloads/` | Raw `.sql.gz` dump files (input for `build`). |
| `data/processed/` | Binary CSR graph and title index (input for `search`/`serve`). |

## License

MIT

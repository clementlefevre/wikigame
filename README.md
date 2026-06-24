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

# 2. Download the Wikipedia dumps (~10 GB, resumes interrupted downloads)
./target/release/wikigame download

# 3. Parse the dumps and build the graph (one-time, slow).
#    The raw .sql.gz files are deleted automatically as they are parsed.
./target/release/wikigame build

# 4a. Search from the command line
./target/release/wikigame search "Kevin Bacon" "Philosophy"

# 4b. Or start the web server and open http://localhost:8080
./target/release/wikigame serve
```

The web UI shows the shortest path as an interactive 3-D scene. Path nodes are drawn in blue, and a cloud of neighboring articles (up to a configurable limit) is drawn in purple around each path node.

## Commands

| Command | Description |
|---|---|
| `download [--output <dir>]` | Download the three SQL dumps from Wikimedia. |
| `build [--downloads <dir>] [--output <dir>] [--no-download] [--keep-dumps]` | Parse dumps and write binary graph files. Use `--keep-dumps` to keep the raw `.sql.gz` files after building. |
| `search <from> <to> [-i] [--data <dir>]` | Find the shortest path between two articles. Pass `-i` to keep the graph in memory for repeated queries. |
| `serve [--port <port>] [--data <dir>]` | Start the local web server (default port 8080). |

## API endpoints (used by the web UI)

| Endpoint | Method | Body | Response |
|---|---|---|---|
| `/search` | POST | `{ "from": "...", "to": "..." }` | `{ "path": [...], "hops": N, "ms": N, "error?": "..." }` |
| `/neighbors` | POST | `{ "title": "...", "limit": 30 }` | `{ "title": "...", "total": N, "neighbors": [...], "error?": "..." }` |

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

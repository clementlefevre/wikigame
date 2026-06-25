/// Setup orchestrator: download → build → cleanup → load graph.
///
/// Runs on a dedicated OS thread because the download/build pipeline is
/// blocking (CPU + IO heavy). Progress events are published to a
/// `ProgressReporter` whose broadcast channel the SSE endpoint subscribes to.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    build_cmd::TitleIndex,
    download,
    graph::{self, LoadedGraph},
    progress::ProgressReporter,
    stats::GraphStats,
};

/// Whether the processed graph is already present on disk.
pub fn graph_ready(data_dir: &Path) -> bool {
    let files = [
        "title_index.bin",
        "fwd_offsets.bin",
        "fwd_columns.bin",
        "bwd_offsets.bin",
        "bwd_columns.bin",
    ];
    files.iter().all(|f| data_dir.join(f).exists())
}

/// Run the full setup pipeline synchronously. Emits progress events to the
/// reporter. Panics on failure (the caller wraps in catch_unwind).
pub fn run_setup(downloads_dir: &Path, data_dir: &Path, reporter: &ProgressReporter) {
    // 1. Download (skips already-complete files).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for download");
    rt.block_on(download::download_all(downloads_dir, reporter));

    // 2. Build CSR binaries, deleting dumps as they're consumed.
    crate::build_cmd::run(downloads_dir, data_dir, true, reporter);

    // 3. Clean up the now-empty downloads directory.
    if downloads_dir.exists() {
        let _ = std::fs::remove_dir_all(downloads_dir);
        reporter.log("Setup", format!("Cleaned up {}", downloads_dir.display()));
    }

    reporter.done("Setup complete. Graph ready to search.");
}

/// Load the graph + title index from disk. Returns `None` if files missing.
pub fn load_graph(data_dir: &Path) -> Option<(Arc<LoadedGraph>, Arc<TitleIndex>)> {
    if !graph_ready(data_dir) {
        return None;
    }
    let titles = load_title_index(data_dir)?;
    let graph = graph::load(data_dir);
    Some((Arc::new(graph), Arc::new(titles)))
}

pub fn load_title_index(data_dir: &Path) -> Option<TitleIndex> {
    let path = data_dir.join("title_index.bin");
    let f = std::fs::File::open(&path).ok()?;
    bincode::deserialize_from(std::io::BufReader::new(f)).ok()
}

/// The shared, mutable state of the running app.
///
/// - `NeedsSetup`: graph files not present yet; UI shows the setup wizard.
/// - `Building`:   a setup run is in progress; SSE streams progress.
/// - `Ready`:      graph loaded; `/search` and `/neighbors` are live.
pub enum AppState {
    NeedsSetup,
    Building,
    Ready {
        graph: Arc<LoadedGraph>,
        titles: Arc<TitleIndex>,
        stats: Arc<tokio::sync::Mutex<Option<Arc<GraphStats>>>>,
    },
    Error(String),
}

impl AppState {
    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        matches!(self, AppState::Ready { .. })
    }
}

/// Shared, thread-safe wrapper around `AppState` plus the live progress
/// reporter (so SSE clients can subscribe even while building).
pub struct AppHandle {
    pub state: Arc<Mutex<AppState>>,
    pub reporter: ProgressReporter,
    pub downloads_dir: PathBuf,
    pub data_dir: PathBuf,
}

impl AppHandle {
    pub fn new(downloads_dir: PathBuf, data_dir: PathBuf) -> Self {
        let initial = if graph_ready(&data_dir) {
            match load_graph(&data_dir) {
                Some((graph, titles)) => AppState::Ready {
                    graph,
                    titles,
                    stats: Arc::new(tokio::sync::Mutex::new(None)),
                },
                None => AppState::NeedsSetup,
            }
        } else {
            AppState::NeedsSetup
        };
        AppHandle {
            state: Arc::new(Mutex::new(initial)),
            reporter: ProgressReporter::standalone(256),
            downloads_dir,
            data_dir,
        }
    }

    /// Best-effort synchronous check (used before the async runtime starts).
    #[allow(dead_code)]
    pub fn blocking_is_ready(&self) -> bool {
        // Try a try_lock; if contended, assume not ready (caller is at startup).
        match self.state.try_lock() {
            Ok(g) => g.is_ready(),
            Err(_) => false,
        }
    }
}

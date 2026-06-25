/// Progress reporting via a tokio broadcast channel.
///
/// The download/build pipeline emits `ProgressEvent`s. Both the CLI
/// (prints to stdout) and the web setup wizard (SSE stream to the browser)
/// subscribe to the same channel.
use serde::Serialize;
use tokio::sync::broadcast;

#[derive(Clone, Serialize)]
pub struct ProgressEvent {
    /// `phase` | `progress` | `log` | `done` | `error`
    pub kind: &'static str,
    /// Human-readable phase label, e.g. "Downloading", "Parsing page", "Building CSR".
    pub phase: String,
    /// Free-form message line.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
}

impl ProgressEvent {
    pub fn phase(phase: &str, message: impl Into<String>) -> Self {
        ProgressEvent {
            kind: "phase",
            phase: phase.to_string(),
            message: message.into(),
            current: None,
            total: None,
        }
    }
    pub fn progress(phase: &str, message: impl Into<String>, current: u64, total: u64) -> Self {
        ProgressEvent {
            kind: "progress",
            phase: phase.to_string(),
            message: message.into(),
            current: Some(current),
            total: Some(total),
        }
    }
    pub fn log(phase: &str, message: impl Into<String>) -> Self {
        ProgressEvent {
            kind: "log",
            phase: phase.to_string(),
            message: message.into(),
            current: None,
            total: None,
        }
    }
    pub fn done(message: impl Into<String>) -> Self {
        ProgressEvent {
            kind: "done",
            phase: String::new(),
            message: message.into(),
            current: None,
            total: None,
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        ProgressEvent {
            kind: "error",
            phase: String::new(),
            message: message.into(),
            current: None,
            total: None,
        }
    }
}

/// Clone-able handle used by the pipeline to emit events.
#[derive(Clone)]
pub struct ProgressReporter {
    tx: broadcast::Sender<ProgressEvent>,
}

impl ProgressReporter {
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> (Self, broadcast::Receiver<ProgressEvent>) {
        let (tx, rx) = broadcast::channel(capacity);
        (ProgressReporter { tx }, rx)
    }

    /// Create a reporter with no live receiver yet (events are buffered until
    /// a subscriber attaches via `subscribe`).
    pub fn standalone(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        // Drop the receiver so the channel starts in "no receivers" mode;
        // send() will still succeed and buffer the last `capacity` events.
        ProgressReporter { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ProgressEvent> {
        self.tx.subscribe()
    }

    /// Emit an event. Fails silently if there are no subscribers (e.g. CLI
    /// mode where nobody is listening to the broadcast — the caller should
    /// also print to stdout itself).
    pub fn emit(&self, event: ProgressEvent) {
        let _ = self.tx.send(event);
    }

    pub fn phase(&self, phase: &str, message: impl Into<String>) {
        self.emit(ProgressEvent::phase(phase, message));
    }
    pub fn progress(&self, phase: &str, message: impl Into<String>, current: u64, total: u64) {
        self.emit(ProgressEvent::progress(phase, message, current, total));
    }
    pub fn log(&self, phase: &str, message: impl Into<String>) {
        self.emit(ProgressEvent::log(phase, message));
    }
    pub fn done(&self, message: impl Into<String>) {
        self.emit(ProgressEvent::done(message));
    }
    pub fn error(&self, message: impl Into<String>) {
        self.emit(ProgressEvent::error(message));
    }
}

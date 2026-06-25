/// Download the three Wikipedia SQL dump files needed to build the graph.
///
/// Features:
///   - Resumes interrupted downloads using the HTTP `Range` header.
///   - Reports byte-level progress via the ProgressReporter.
///   - Downloads files sequentially.
use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::Client;

use crate::progress::ProgressReporter;

// ── File list ──────────────────────────────────────────────────────────────────

pub const BASE_URL: &str = "https://dumps.wikimedia.org/enwiki/latest";

/// (filename, friendly label, approximate size in bytes for UI hints)
pub const DUMPS: &[(&str, &str, u64)] = &[
    ("enwiki-latest-page.sql.gz",       "page",       2_400_000_000),
    ("enwiki-latest-linktarget.sql.gz", "linktarget", 1_400_000_000),
    ("enwiki-latest-pagelinks.sql.gz",  "pagelinks",  6_900_000_000),
];

// ── Public entry point ─────────────────────────────────────────────────────────

/// Download all three dump files into `dest_dir`.  Skips files that are already
/// fully downloaded (checks by comparing expected Content-Length to local size).
/// Resumes partial downloads using the HTTP `Range` header.
pub async fn download_all(dest_dir: &Path, reporter: &ProgressReporter) {
    fs::create_dir_all(dest_dir)
        .unwrap_or_else(|e| panic!("Cannot create downloads dir {:?}: {}", dest_dir, e));

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .user_agent("wikigame/0.1 (https://github.com/wikigame)")
        .build()
        .expect("Failed to build HTTP client");

    for (i, (filename, label, approx)) in DUMPS.iter().enumerate() {
        let url = format!("{}/{}", BASE_URL, filename);
        let dest = dest_dir.join(filename);
        reporter.phase(
            "Downloading",
            format!("[{}/{}] {} (≈{:.1} GB)", i + 1, DUMPS.len(), label, *approx as f64 / 1e9),
        );
        download_file(&client, &url, &dest, label, reporter).await;
    }

    reporter.log("Downloading", "All dumps downloaded.");
}

// ── Per-file download ──────────────────────────────────────────────────────────

async fn download_file(client: &Client, url: &str, dest: &Path, label: &str, reporter: &ProgressReporter) {
    // Find out how many bytes we already have on disk.
    let existing_bytes = dest
        .metadata()
        .map(|m| m.len())
        .unwrap_or(0);

    // HEAD request to get total size.
    let total_size = match client.head(url).send().await {
        Ok(resp) => resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
        Err(e) => {
            let msg = format!(
                "HEAD request failed: {}{}",
                e,
                e.source().map(|s| format!(" (caused by: {})", s)).unwrap_or_default()
            );
            reporter.error(msg.clone());
            panic!("{}", msg);
        }
    };

    if total_size > 0 && existing_bytes >= total_size {
        reporter.progress("Downloading", format!("{} already complete", label), total_size, total_size);
        return;
    }

    if existing_bytes > 0 && total_size > 0 {
        reporter.log(
            "Downloading",
            format!("Resuming {} from {} / {} bytes ({:.1}%)", label, existing_bytes, total_size,
                existing_bytes as f64 / total_size as f64 * 100.0),
        );
    }

    // Build the GET request with optional Range header for resuming.
    let mut req = client.get(url);
    if existing_bytes > 0 {
        req = req.header(
            reqwest::header::RANGE,
            format!("bytes={}-", existing_bytes),
        );
    }

    let resp = req
        .send()
        .await
        .unwrap_or_else(|e| {
            let mut msg = format!("Failed to connect to {}: {}", url, e);
            let mut source = e.source();
            while let Some(s) = source {
                msg.push_str(&format!("\n  caused by: {}", s));
                source = s.source();
            }
            reporter.error(msg.clone());
            panic!("{}", msg);
        });

    let status = resp.status();
    if !status.is_success() && status.as_u16() != 206 {
        let msg = format!("Server returned {} for {}", status, url);
        reporter.error(msg.clone());
        panic!("{}", msg);
    }

    // If server ignored our Range header (200 instead of 206), restart from 0.
    let resume = status.as_u16() == 206;
    let file_existing = if resume { existing_bytes } else { 0 };

    // Content-Length from the GET response (bytes *remaining*, not total).
    let remaining = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let display_total = if total_size > 0 {
        total_size
    } else {
        file_existing + remaining
    };

    reporter.progress("Downloading", format!("{}", label), file_existing, display_total);

    // Open file for appending (or truncate if not resuming).
    let mut file = if resume && file_existing > 0 {
        OpenOptions::new()
            .append(true)
            .open(dest)
            .unwrap_or_else(|e| panic!("Cannot open {:?} for append: {}", dest, e))
    } else {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest)
            .unwrap_or_else(|e| panic!("Cannot create {:?}: {}", dest, e))
    };

    // Stream body to disk.
    let mut stream = resp.bytes_stream();
    let mut pos = file_existing;
    let mut last_emit = pos;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap_or_else(|e| panic!("Stream error downloading {}: {}", url, e));
        file.write_all(&chunk)
            .unwrap_or_else(|e| panic!("Write error to {:?}: {}", dest, e));
        pos += chunk.len() as u64;
        // Emit progress at most ~10×/s to avoid flooding the channel.
        if pos - last_emit >= display_total / 200 || pos == display_total {
            reporter.progress("Downloading", format!("{}", label), pos, display_total);
            last_emit = pos;
        }
    }

    reporter.progress("Downloading", format!("{} saved", label), display_total, display_total);
}

// ── Helper: check all 3 files exist ───────────────────────────────────────────

/// Returns `true` only if all three dump files exist and are non-empty.
pub fn all_present(downloads_dir: &Path) -> bool {
    DUMPS.iter().all(|(filename, _, _)| {
        let p: PathBuf = downloads_dir.join(filename);
        p.exists() && p.metadata().map(|m| m.len() > 0).unwrap_or(false)
    })
}

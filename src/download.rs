/// Download the three Wikipedia SQL dump files needed to build the graph.
///
/// Features:
///   - Resumes interrupted downloads using the HTTP `Range` header.
///   - Shows a per-file progress bar (bytes / total size / speed).
///   - Downloads files sequentially so you can clearly see which one is active.
use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;

// ── File list ──────────────────────────────────────────────────────────────────

const BASE_URL: &str = "https://dumps.wikimedia.org/enwiki/latest";

/// (filename, friendly label)
const DUMPS: &[(&str, &str)] = &[
    ("enwiki-latest-page.sql.gz",        "page       (~2.4 GB)"),
    ("enwiki-latest-linktarget.sql.gz",  "linktarget (~1.4 GB)"),
    ("enwiki-latest-pagelinks.sql.gz",   "pagelinks  (~6.9 GB)"),
];

// ── Public entry point ─────────────────────────────────────────────────────────

/// Download all three dump files into `dest_dir`.  Skips files that are already
/// fully downloaded (checks by comparing expected Content-Length to local size).
/// Resumes partial downloads using the HTTP `Range` header.
pub async fn download_all(dest_dir: &Path) {
    fs::create_dir_all(dest_dir)
        .unwrap_or_else(|e| panic!("Cannot create downloads dir {:?}: {}", dest_dir, e));

    let client = Client::builder()
        .timeout(Duration::from_secs(0)) // no request-level timeout — files are huge
        .connect_timeout(Duration::from_secs(30))
        .user_agent("wikigame/0.1 (https://github.com/wikigame)")
        .build()
        .expect("Failed to build HTTP client");

    for (filename, label) in DUMPS {
        let url = format!("{}/{}", BASE_URL, filename);
        let dest = dest_dir.join(filename);
        println!("\n[{label}]");
        download_file(&client, &url, &dest).await;
    }

    println!("\nAll dumps downloaded to {:?}", dest_dir);
}

// ── Per-file download ──────────────────────────────────────────────────────────

async fn download_file(client: &Client, url: &str, dest: &Path) {
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
            eprintln!("  HEAD request failed: {}{}",
                e,
                e.source().map(|s| format!(" (caused by: {})", s)).unwrap_or_default()
            );
            0
        }
    };

    if total_size > 0 && existing_bytes >= total_size {
        println!("  Already complete ({} bytes). Skipping.", existing_bytes);
        return;
    }

    if existing_bytes > 0 && total_size > 0 {
        println!(
            "  Resuming from {} / {} bytes ({:.1}%)",
            existing_bytes,
            total_size,
            existing_bytes as f64 / total_size as f64 * 100.0
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
            // Walk the error chain so the real root cause (TLS, DNS, etc.) is visible.
            let mut msg = format!("Failed to connect to {}: {}", url, e);
            let mut source = e.source();
            while let Some(s) = source {
                msg.push_str(&format!("\n  caused by: {}", s));
                source = s.source();
            }
            panic!("{}", msg);
        });

    let status = resp.status();
    if !status.is_success() && status.as_u16() != 206 {
        panic!("Server returned {} for {}", status, url);
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

    // Progress bar.
    let pb = ProgressBar::new(display_total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  {spinner:.green} [{elapsed_precise}] [{bar:45.cyan/blue}] \
                 {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_position(file_existing);

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
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap_or_else(|e| panic!("Stream error downloading {}: {}", url, e));
        file.write_all(&chunk)
            .unwrap_or_else(|e| panic!("Write error to {:?}: {}", dest, e));
        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message("done");
    println!("  Saved to {:?}", dest);
}

// ── Helper: check all 3 files exist ───────────────────────────────────────────

/// Returns `true` only if all three dump files exist and are non-empty.
pub fn all_present(downloads_dir: &Path) -> bool {
    DUMPS.iter().all(|(filename, _)| {
        let p: PathBuf = downloads_dir.join(filename);
        p.exists() && p.metadata().map(|m| m.len() > 0).unwrap_or(false)
    })
}

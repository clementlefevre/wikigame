/// Download prebuilt `.bin` graph files from a mirror (e.g. Cloudflare R2)
/// instead of running the full download+build pipeline locally.
///
/// Mirrors `download.rs` patterns: streaming download with resume via HTTP
/// Range, progress reporting, skips files already complete.
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::Client;

use crate::progress::ProgressReporter;

/// Default mirror base URL. Override with `--base-url` on the CLI or the
/// `WIKIGAME_MIRROR_URL` environment variable. Points at a Cloudflare R2
/// bucket (or any S3-compatible public endpoint) that hosts the five
/// processed binary files produced by `wikigame build`.
pub const DEFAULT_MIRROR_URL: &str = "https://wikigame-graph.cloudflareaccess.com";

/// The five processed files that constitute a ready-to-use graph.
pub const BIN_FILES: &[&str] = &[
    "title_index.bin",
    "fwd_offsets.bin",
    "fwd_columns.bin",
    "bwd_offsets.bin",
    "bwd_columns.bin",
];

/// Resolve the mirror base URL: CLI flag > env var > default constant.
pub fn mirror_url(override_url: Option<&str>) -> String {
    if let Some(u) = override_url {
        return u.trim_end_matches('/').to_string();
    }
    if let Ok(u) = std::env::var("WIKIGAME_MIRROR_URL") {
        return u.trim_end_matches('/').to_string();
    }
    DEFAULT_MIRROR_URL.to_string()
}

/// Download all five `.bin` files into `dest_dir` from the mirror.
/// Skips files already fully downloaded; resumes partial downloads.
pub async fn fetch_all(
    dest_dir: &Path,
    base_url: &str,
    reporter: &ProgressReporter,
) {
    fs::create_dir_all(dest_dir)
        .unwrap_or_else(|e| panic!("Cannot create data dir {:?}: {}", dest_dir, e));

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .user_agent("wikigame/0.2 (https://github.com/clementlefevre/wikigame)")
        .build()
        .expect("Failed to build HTTP client");

    for (i, filename) in BIN_FILES.iter().enumerate() {
        let url = format!("{}/{}", base_url, filename);
        let dest = dest_dir.join(filename);
        reporter.phase(
            "Fetching",
            format!("[{}/{}] {}", i + 1, BIN_FILES.len(), filename),
        );
        download_file(&client, &url, &dest, filename, reporter).await;
    }

    reporter.log("Fetching", "All graph files fetched.");
}

async fn download_file(
    client: &Client,
    url: &str,
    dest: &Path,
    label: &str,
    reporter: &ProgressReporter,
) {
    let existing_bytes = dest.metadata().map(|m| m.len()).unwrap_or(0);

    let total_size = match client.head(url).send().await {
        Ok(resp) => resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
        Err(e) => {
            let msg = format!("HEAD request failed for {}: {}", url, e);
            reporter.error(msg.clone());
            panic!("{}", msg);
        }
    };

    if total_size > 0 && existing_bytes >= total_size {
        reporter.progress(
            "Fetching",
            format!("{} already complete", label),
            total_size,
            total_size,
        );
        return;
    }

    if existing_bytes > 0 && total_size > 0 {
        reporter.log(
            "Fetching",
            format!(
                "Resuming {} from {} / {} bytes ({:.1}%)",
                label,
                existing_bytes,
                total_size,
                existing_bytes as f64 / total_size as f64 * 100.0
            ),
        );
    }

    let mut req = client.get(url);
    if existing_bytes > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={}-", existing_bytes));
    }

    let resp = req.send().await.unwrap_or_else(|e| {
        let msg = format!("Failed to connect to {}: {}", url, e);
        reporter.error(msg.clone());
        panic!("{}", msg);
    });

    let status = resp.status();
    if !status.is_success() && status.as_u16() != 206 {
        let msg = format!("Server returned {} for {}", status, url);
        reporter.error(msg.clone());
        panic!("{}", msg);
    }

    let resume = status.as_u16() == 206;
    let file_existing = if resume { existing_bytes } else { 0 };

    let remaining = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let total = if resume {
        file_existing + remaining
    } else {
        remaining
    };

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(resume)
        .open(dest)
        .unwrap_or_else(|e| panic!("Cannot open {:?}: {}", dest, e));

    let mut downloaded = file_existing;
    let mut stream = resp.bytes_stream();
    let mut last_report = std::time::Instant::now();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.unwrap_or_else(|e| {
            let msg = format!("Download error on {}: {}", label, e);
            reporter.error(msg.clone());
            panic!("{}", msg);
        });
        file.write_all(&chunk).unwrap_or_else(|e| {
            panic!("Write error on {:?}: {}", dest, e);
        });
        downloaded += chunk.len() as u64;

        if last_report.elapsed().as_millis() > 200 {
            reporter.progress(
                "Fetching",
                format!(
                    "{}  {:.1} / {:.1} GB",
                    label,
                    downloaded as f64 / 1e9,
                    total as f64 / 1e9
                ),
                total,
                downloaded,
            );
            last_report = std::time::Instant::now();
        }
    }

    reporter.progress("Fetching", format!("{} done", label), total, downloaded);
}

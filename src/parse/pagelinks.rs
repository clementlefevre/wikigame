/// Parse `enwiki-latest-pagelinks.sql.gz` and write `edges.tmp`.
///
/// Schema (MW 1.43+):
///   pl_from           INT UNSIGNED    col 0  (wiki page_id of the source article)
///   pl_from_namespace INT             col 1  (namespace of the source page; added MW 1.19/2012)
///   pl_target_id      BIGINT UNSIGNED col 2  (FK → linktarget.lt_id; added MW 1.41/2024)
///
/// IMPORTANT: pl_from_namespace (col 1) was added before pl_target_id (col 2),
/// so the column order in the dump is pl_from, pl_from_namespace, pl_target_id.
///
/// We resolve both sides to compact IDs using the pre-built maps.
/// Unresolvable edges (link to non-article, redirect, or unknown page) are
/// silently dropped — this is correct behaviour.
///
/// Output: writes raw little-endian `(src_cid: u32, dst_cid: u32)` pairs to
/// `edges_path`.  Returns `(num_nodes, num_edges_written)`.
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use hashbrown::HashMap;
use indicatif::{ProgressBar, ProgressStyle};

pub fn parse_and_write(
    path: &Path,
    edges_path: &Path,
    wiki_id_to_cid: &HashMap<u32, u32>,
    lt_to_cid: &HashMap<u64, u32>,
) -> u64 {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    pb.set_message("Parsing pagelinks.sql.gz …");

    let out_file = File::create(edges_path)
        .unwrap_or_else(|e| panic!("Cannot create {:?}: {}", edges_path, e));
    let mut writer = BufWriter::with_capacity(64 * 1024 * 1024, out_file);

    let mut total_rows: u64 = 0;
    let mut written: u64 = 0;

    for row in super::rows(path) {
        total_rows += 1;

        if row.len() < 2 {
            continue;
        }

        // col 1: pl_from_namespace — only keep links originating from NS 0
        // (must have at least 3 columns for this schema variant)
        if row.len() < 3 {
            continue;
        }
        let from_ns = match row[1].as_i64() {
            Some(n) => n,
            None => continue,
        };
        if from_ns != 0 {
            continue;
        }

        // col 0: pl_from (wiki page_id)
        let src_wiki_id = match row[0].as_i64() {
            Some(n) if n > 0 => n as u32,
            _ => continue,
        };
        let src_cid = match wiki_id_to_cid.get(&src_wiki_id) {
            Some(&c) => c,
            None => continue,
        };

        // col 2: pl_target_id (FK into linktarget table)
        let lt_id = match row[2].as_i64() {
            Some(n) if n > 0 => n as u64,
            _ => continue,
        };
        let dst_cid = match lt_to_cid.get(&lt_id) {
            Some(&c) => c,
            None => continue,
        };

        // Skip self-links
        if src_cid == dst_cid {
            continue;
        }

        // Write as two little-endian u32s
        writer.write_all(&src_cid.to_le_bytes()).expect("write src");
        writer.write_all(&dst_cid.to_le_bytes()).expect("write dst");
        written += 1;

        if total_rows % 5_000_000 == 0 {
            pb.set_message(format!(
                "Parsing pagelinks.sql.gz … {} rows, {} edges written",
                total_rows, written
            ));
        }
    }

    writer.flush().expect("flush edges.tmp");

    pb.finish_with_message(format!(
        "pagelinks.sql.gz done — {} edges written ({} rows processed)",
        written, total_rows
    ));

    written
}

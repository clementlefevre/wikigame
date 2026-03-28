/// Build command: orchestrates parsing → CSR binary construction.
///
/// Pipeline:
///   1. Parse page.sql.gz   → compact ID mapping + title lookup tables
///   2. Parse linktarget.sql.gz → lt_id → compact_id map (in-memory only)
///   3. Parse pagelinks.sql.gz  → write edges.tmp (flat u32 pairs)
///   4. Two-pass CSR build:
///      Pass 1: count out-degree (fwd) and in-degree (bwd) from edges.tmp
///      Pass 2: scatter-fill both CSR column arrays
///   5. Write processed binaries; delete edges.tmp
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};

use crate::parse;

// ── Serialisable title index ───────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct TitleIndex {
    pub titles: Vec<String>,
    pub title_to_cid: hashbrown::HashMap<String, u32>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub fn run(downloads_dir: &Path, output_dir: &Path) {
    fs::create_dir_all(output_dir)
        .unwrap_or_else(|e| panic!("Cannot create output dir {:?}: {}", output_dir, e));

    // ── Step 1: parse page dump ───────────────────────────────────────────────
    let page_path = downloads_dir.join("enwiki-latest-page.sql.gz");
    assert!(
        page_path.exists(),
        "Missing: {}  (download from https://dumps.wikimedia.org/enwiki/latest/)",
        page_path.display()
    );

    let page_index = parse::page::parse(&page_path);
    let num_nodes = page_index.titles.len() as u32;
    println!("  Articles: {}", num_nodes);

    // Persist titles + title_to_cid
    let title_index = TitleIndex {
        titles: page_index.titles.clone(),
        title_to_cid: page_index.title_to_cid.clone(),
    };
    write_bincode(output_dir.join("title_index.bin"), &title_index);
    println!("  Wrote title_index.bin");

    // ── Step 2: parse linktarget dump ─────────────────────────────────────────
    let lt_path = downloads_dir.join("enwiki-latest-linktarget.sql.gz");
    assert!(
        lt_path.exists(),
        "Missing: {}",
        lt_path.display()
    );
    let lt_to_cid = parse::linktarget::parse(&lt_path, &page_index.title_to_cid);
    println!("  Link targets mapped: {}", lt_to_cid.len());

    // ── Step 3: parse pagelinks, write edges.tmp ──────────────────────────────
    let pl_path = downloads_dir.join("enwiki-latest-pagelinks.sql.gz");
    assert!(
        pl_path.exists(),
        "Missing: {}",
        pl_path.display()
    );
    let edges_path = output_dir.join("edges.tmp");
    let num_edges = parse::pagelinks::parse_and_write(
        &pl_path,
        &edges_path,
        &page_index.wiki_id_to_cid,
        &lt_to_cid,
    );
    println!("  Edges written: {}", num_edges);

    // Free the big maps — we don't need them anymore.
    drop(lt_to_cid);
    drop(page_index.wiki_id_to_cid);

    // ── Step 4: two-pass CSR construction ─────────────────────────────────────
    println!("  Building CSR (pass 1: degree counting) …");
    let mut fwd_degree = vec![0u32; num_nodes as usize + 1];
    let mut bwd_degree = vec![0u32; num_nodes as usize + 1];
    stream_edges(&edges_path, num_edges, |src, dst| {
        fwd_degree[src as usize] += 1;
        bwd_degree[dst as usize] += 1;
    });

    // Prefix-sum → offsets arrays (length = num_nodes + 1)
    let fwd_offsets = degree_to_offsets(fwd_degree);
    let bwd_offsets = degree_to_offsets(bwd_degree);

    println!("  Building CSR (pass 2: scatter fill) …");
    // Allocate column arrays
    let mut fwd_columns = vec![0u32; num_edges as usize];
    let mut bwd_columns = vec![0u32; num_edges as usize];

    // cursors: position within each node's slot in the columns array
    let mut fwd_cursor: Vec<u64> = fwd_offsets[..fwd_offsets.len() - 1].to_vec();
    let mut bwd_cursor: Vec<u64> = bwd_offsets[..bwd_offsets.len() - 1].to_vec();

    stream_edges(&edges_path, num_edges, |src, dst| {
        let fi = fwd_cursor[src as usize] as usize;
        fwd_columns[fi] = dst;
        fwd_cursor[src as usize] += 1;

        let bi = bwd_cursor[dst as usize] as usize;
        bwd_columns[bi] = src;
        bwd_cursor[dst as usize] += 1;
    });

    drop(fwd_cursor);
    drop(bwd_cursor);

    // ── Step 5: write CSR binaries ────────────────────────────────────────────
    println!("  Writing CSR binaries …");
    write_u64_array(output_dir.join("fwd_offsets.bin"), &fwd_offsets);
    drop(fwd_offsets);
    write_u32_array(output_dir.join("fwd_columns.bin"), &fwd_columns);
    drop(fwd_columns);

    write_u64_array(output_dir.join("bwd_offsets.bin"), &bwd_offsets);
    drop(bwd_offsets);
    write_u32_array(output_dir.join("bwd_columns.bin"), &bwd_columns);
    drop(bwd_columns);

    // Delete temporary edge file
    fs::remove_file(&edges_path).ok();
    println!("  Deleted edges.tmp");
    println!("Build complete. Processed files are in {:?}", output_dir);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn stream_edges<F: FnMut(u32, u32)>(path: &Path, num_edges: u64, mut f: F) {
    let pb = ProgressBar::new(num_edges);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan} [{elapsed_precise}] [{bar:40}] {pos}/{len} edges")
            .unwrap()
            .progress_chars("=> "),
    );

    let file = File::open(path).unwrap_or_else(|e| panic!("Cannot open edges.tmp: {}", e));
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
    let mut buf = [0u8; 8];
    let mut count = 0u64;

    loop {
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Read error in edges.tmp: {}", e),
        }
        let src = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let dst = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        f(src, dst);
        count += 1;
        if count % 10_000_000 == 0 {
            pb.set_position(count);
        }
    }
    pb.finish_with_message(format!("Streamed {} edges", count));
}

fn degree_to_offsets(degree: Vec<u32>) -> Vec<u64> {
    let mut offsets: Vec<u64> = Vec::with_capacity(degree.len() + 1);
    let mut acc: u64 = 0;
    for &d in &degree {
        offsets.push(acc);
        acc += d as u64;
    }
    offsets.push(acc);
    offsets
}

fn write_bincode<T: Serialize>(path: PathBuf, value: &T) {
    let f = File::create(&path).unwrap_or_else(|e| panic!("Cannot create {:?}: {}", path, e));
    let w = BufWriter::new(f);
    bincode::serialize_into(w, value).unwrap_or_else(|e| panic!("bincode error: {}", e));
}

fn write_u64_array(path: PathBuf, arr: &[u64]) {
    let f = File::create(&path).unwrap_or_else(|e| panic!("Cannot create {:?}: {}", path, e));
    let mut w = BufWriter::with_capacity(16 * 1024 * 1024, f);
    for &v in arr {
        w.write_all(&v.to_le_bytes()).expect("write u64");
    }
}

fn write_u32_array(path: PathBuf, arr: &[u32]) {
    let f = File::create(&path).unwrap_or_else(|e| panic!("Cannot create {:?}: {}", path, e));
    let mut w = BufWriter::with_capacity(64 * 1024 * 1024, f);
    for &v in arr {
        w.write_all(&v.to_le_bytes()).expect("write u32");
    }
}

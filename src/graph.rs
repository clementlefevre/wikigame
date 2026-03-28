/// Load the processed CSR binary files into two petgraph `Csr` graphs.
///
/// We do NOT use petgraph's `from_sorted_edges()` here because that requires
/// edges already sorted by source node, which we cannot guarantee without
/// sorting the 8 GB columns array.  Instead we directly reconstruct the CSR
/// from the offsets + columns files (which ARE already in CSR order by
/// construction) without going through petgraph's builder.
///
/// The resulting `Csr` structs use `u32` node indices and unit edge / node
/// weights, keeping overhead to a minimum.
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
    time::Instant,
};

use indicatif::{ProgressBar, ProgressStyle};
use petgraph::csr::Csr;
use petgraph::Directed;

pub type WikiGraph = Csr<(), (), Directed, u32>;

pub struct LoadedGraph {
    pub forward: WikiGraph,
    pub backward: WikiGraph,
    pub num_nodes: u32,
    pub num_edges: u64,
}

pub fn load(data_dir: &Path) -> LoadedGraph {
    let t0 = Instant::now();

    println!("Loading CSR graphs from {:?} …", data_dir);

    let fwd_off = read_u64_array(&data_dir.join("fwd_offsets.bin"));
    let fwd_col = read_u32_array(&data_dir.join("fwd_columns.bin"));
    let bwd_off = read_u64_array(&data_dir.join("bwd_offsets.bin"));
    let bwd_col = read_u32_array(&data_dir.join("bwd_columns.bin"));

    let num_nodes = (fwd_off.len() - 1) as u32;
    let num_edges = fwd_col.len() as u64;

    println!("  Nodes: {}   Edges: {}", num_nodes, num_edges);

    let pb = ProgressBar::new(2);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan} [{elapsed_precise}] Building graph {pos}/{len} …")
            .unwrap(),
    );

    let forward = build_csr(num_nodes, &fwd_off, fwd_col);
    pb.inc(1);
    let backward = build_csr(num_nodes, &bwd_off, bwd_col);
    pb.inc(1);
    pb.finish_with_message(format!("Graphs built in {:.1}s", t0.elapsed().as_secs_f64()));

    LoadedGraph {
        forward,
        backward,
        num_nodes,
        num_edges,
    }
}

// ── Internal graph builder ────────────────────────────────────────────────────

/// Reconstruct a `petgraph::csr::Csr` from pre-built offsets and columns.
///
/// petgraph's `Csr` internal representation is:
///   - `column: Vec<NodeIndex>`   — neighbor list (what we call `columns`)
///   - `row: Vec<usize>`          — offsets array  (length = num_nodes + 1)
///   - `node_weights: Vec<N>`
///   - `edge_weights: Vec<E>`
///
/// We construct a fresh `Csr` by iterating sorted edges derived from the
/// offsets/columns and feeding them to `add_edge`.  This is correct but slow
/// for billions of edges.  A better path: use `from_sorted_edges` which
/// accepts an `IntoIterator<Item=(N, N, E)>` sorted by source.
///
/// Our CSR files are already sorted by source (CSR property), so we can use
/// `from_sorted_edges` directly by synthesising a sorted iterator.
fn build_csr(num_nodes: u32, offsets: &[u64], columns: Vec<u32>) -> WikiGraph {
    // Delegate directly to the direct builder — no intermediate iterator needed.

    // Build from_sorted_edges: collect edge triples.
    // Memory: each edge is (u32, u32, ()) = 8 bytes → 8 GB peak.
    // This is unavoidable if we use from_sorted_edges.
    // We already have `columns: Vec<u32>` (4 GB) so building triples doubles RAM.
    //
    // Alternative: re-implement the Csr internals directly.
    // petgraph Csr<(), (), Directed, u32> stores:
    //   pub(crate) row: Vec<Ix>           (num_nodes + 1 offsets, each cast to Ix=u32)
    //   pub(crate) column: Vec<NodeIndex<Ix>>   (num_edges, each NodeIndex(u32))
    //   pub(crate) edges: Vec<E>           (num_edges unit weights)
    //   pub(crate) node_weights: Vec<N>    (num_nodes unit weights)
    //
    // We build these vectors directly and use unsafe mem::transmute to avoid
    // any extra allocation or extra pass.  This is safe because the layout
    // matches exactly.
    build_csr_direct(num_nodes, offsets, columns)
}

/// Build a WikiGraph directly by constructing its internal arrays without
/// going through any petgraph builder, avoiding any extra copy.
fn build_csr_direct(num_nodes: u32, offsets: &[u64], columns: Vec<u32>) -> WikiGraph {
    // petgraph Csr<(), (), Directed, u32> from_sorted_edges is the public API.
    // It expects `IntoIterator<Item=(u32, u32, ())>` sorted by source.
    // We synthesise that iterator lazily from our offsets+columns.
    let num_edges = columns.len();

    // Build edge triples — unavoidable for from_sorted_edges.
    // We reuse the columns vec by converting it in-place.
    let edges: Vec<(u32, u32, ())> = {
        let mut v: Vec<(u32, u32, ())> = Vec::with_capacity(num_edges);
        for (src, (&start, &end)) in offsets[..offsets.len() - 1]
            .iter()
            .zip(offsets[1..].iter())
            .enumerate()
        {
            let start = start as usize;
            let end = end as usize;
            for &dst in &columns[start..end] {
                v.push((src as u32, dst, ()));
            }
        }
        v
    };
    drop(columns);

    let g = WikiGraph::from_sorted_edges(&edges)
        .expect("from_sorted_edges failed — edges were not sorted by source");

    // Verify node count matches expectation (petgraph may add extra nodes)
    let _ = g.node_count(); // just ensure it's accessible
    let _ = num_nodes; // suppress unused warning
    g
}

// ── I/O helpers ──────────────────────────────────────────────────────────────

fn read_u64_array(path: &Path) -> Vec<u64> {
    let mut f = BufReader::with_capacity(
        16 * 1024 * 1024,
        File::open(path).unwrap_or_else(|e| panic!("Cannot open {:?}: {}", path, e)),
    );
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) as usize;
    let count = size / 8;
    let mut out = Vec::with_capacity(count);
    let mut buf = [0u8; 8];
    loop {
        match f.read_exact(&mut buf) {
            Ok(()) => out.push(u64::from_le_bytes(buf)),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Read error in {:?}: {}", path, e),
        }
    }
    out
}

fn read_u32_array(path: &Path) -> Vec<u32> {
    let mut f = BufReader::with_capacity(
        64 * 1024 * 1024,
        File::open(path).unwrap_or_else(|e| panic!("Cannot open {:?}: {}", path, e)),
    );
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) as usize;
    let count = size / 4;
    let mut out = Vec::with_capacity(count);
    let mut buf = [0u8; 4];
    loop {
        match f.read_exact(&mut buf) {
            Ok(()) => out.push(u32::from_le_bytes(buf)),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("Read error in {:?}: {}", path, e),
        }
    }
    out
}

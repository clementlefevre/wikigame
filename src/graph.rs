/// Load the processed CSR binary files into two `WikiCsr` adjacency graphs.
///
/// We use our own lightweight CSR wrapper instead of petgraph to avoid the
/// node-count inference bug in `Csr::from_sorted_edges` (which infers node
/// count as `max_node_index + 1`, giving a 1-node graph when there are 0
/// edges) and to avoid the extra RAM needed to build edge triples.
///
/// The resulting `WikiCsr` structs hold the raw offsets + columns arrays
/// directly — `neighbors(v)` is a single slice into those arrays.
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
    time::Instant,
};

use indicatif::{ProgressBar, ProgressStyle};

/// A compressed sparse row (CSR) directed adjacency graph.
///
/// `offsets[v]..offsets[v+1]` is the range in `columns` that holds the
/// out-neighbours of node `v`.  Node IDs are compact unsigned 32-bit integers.
pub struct WikiCsr {
    offsets: Vec<u64>, // length = num_nodes + 1
    columns: Vec<u32>, // length = num_edges
}

impl WikiCsr {
    /// Out-neighbours of `node` as a slice of compact IDs.
    pub fn neighbors(&self, node: u32) -> &[u32] {
        let start = self.offsets[node as usize] as usize;
        let end = self.offsets[node as usize + 1] as usize;
        &self.columns[start..end]
    }

    pub fn node_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn edge_count(&self) -> usize {
        self.columns.len()
    }
}

pub struct LoadedGraph {
    pub forward: WikiCsr,
    pub backward: WikiCsr,
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
            .template("{spinner:.cyan} [{elapsed_precise}] Loading graph {pos}/{len} …")
            .unwrap(),
    );

    let forward = WikiCsr { offsets: fwd_off, columns: fwd_col };
    pb.inc(1);
    let backward = WikiCsr { offsets: bwd_off, columns: bwd_col };
    pb.inc(1);
    pb.finish_with_message(format!("Graphs loaded in {:.1}s", t0.elapsed().as_secs_f64()));

    LoadedGraph { forward, backward, num_nodes, num_edges }
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

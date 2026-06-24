/// Load the processed CSR binary files into two `WikiCsr` adjacency graphs.
///
/// We use our own lightweight CSR wrapper instead of petgraph to avoid the
/// node-count inference bug in `Csr::from_sorted_edges` (which infers node
/// count as `max_node_index + 1`, giving a 1-node graph when there are 0
/// edges) and to keep memory usage tiny by memory-mapping the binary files
/// instead of copying them into `Vec`s.
use std::{
    fs::File,
    path::Path,
    time::Instant,
};

use memmap2::{Mmap, MmapOptions};

/// A compressed sparse row (CSR) directed adjacency graph backed by a
/// read-only memory map of the offsets and columns binary files.
///
/// `offsets[v]..offsets[v+1]` is the range in `columns` that holds the
/// out-neighbours of node `v`.  Node IDs are compact unsigned 32-bit integers.
pub struct WikiCsr {
    offsets_mmap: Mmap,
    columns_mmap: Mmap,
}

impl WikiCsr {
    fn new(offsets_mmap: Mmap, columns_mmap: Mmap) -> WikiCsr {
        WikiCsr {
            offsets_mmap,
            columns_mmap,
        }
    }

    /// Out-neighbours of `node` as a slice of compact IDs.
    pub fn neighbors(&self, node: u32) -> &[u32] {
        unsafe {
            let offsets = std::slice::from_raw_parts(
                self.offsets_mmap.as_ptr() as *const u64,
                self.offsets_mmap.len() / 8,
            );
            let columns = std::slice::from_raw_parts(
                self.columns_mmap.as_ptr() as *const u32,
                self.columns_mmap.len() / 4,
            );
            let start = offsets[node as usize] as usize;
            let end = offsets[node as usize + 1] as usize;
            &columns[start..end]
        }
    }
}

pub struct LoadedGraph {
    pub forward: WikiCsr,
    pub backward: WikiCsr,
}

pub fn load(data_dir: &Path) -> LoadedGraph {
    let t0 = Instant::now();

    println!("Loading CSR graphs from {:?} …", data_dir);

    let fwd_off = mmap_u64_array(&data_dir.join("fwd_offsets.bin"));
    let fwd_col = mmap_u32_array(&data_dir.join("fwd_columns.bin"));
    let bwd_off = mmap_u64_array(&data_dir.join("bwd_offsets.bin"));
    let bwd_col = mmap_u32_array(&data_dir.join("bwd_columns.bin"));

    let forward = WikiCsr::new(fwd_off, fwd_col);
    let backward = WikiCsr::new(bwd_off, bwd_col);

    let num_nodes = (forward.offsets_mmap.len() / 8).saturating_sub(1) as u32;
    let num_edges = forward.columns_mmap.len() as u64 / 4;

    println!("  Nodes: {}   Edges: {}", num_nodes, num_edges);
    println!("  Graphs loaded in {:.1}s", t0.elapsed().as_secs_f64());

    LoadedGraph { forward, backward }
}

// ── I/O helpers ─────────────────────────────────────────────────────────────────

fn mmap_u64_array(path: &Path) -> Mmap {
    let file = File::open(path)
        .unwrap_or_else(|e| panic!("Cannot open {:?}: {}", path, e));
    unsafe {
        MmapOptions::new()
            .map(&file)
            .unwrap_or_else(|e| panic!("Cannot mmap {:?}: {}", path, e))
    }
}

fn mmap_u32_array(path: &Path) -> Mmap {
    let file = File::open(path)
        .unwrap_or_else(|e| panic!("Cannot open {:?}: {}", path, e));
    unsafe {
        MmapOptions::new()
            .map(&file)
            .unwrap_or_else(|e| panic!("Cannot mmap {:?}: {}", path, e))
    }
}

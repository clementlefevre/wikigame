/// Graph analytics: degree-based "top pages" stats and a hop-distance
/// distribution histogram.
///
/// Everything here runs on the memory-mapped CSR graphs and is O(V + E) or
/// cheaper. Results are serialised by `web.rs` and cached inside `AppState`
/// after the first request (or precomputed right after graph load).
use std::collections::VecDeque;

use serde::Serialize;

use crate::graph::{LoadedGraph, WikiCsr};

/// A single ranked page entry.
#[derive(Serialize, Clone)]
pub struct PageStat {
    pub title: String,
    pub cid: u32,
    pub degree: usize,
}

/// Bucket of the hop-distance histogram.
#[derive(Serialize, Clone)]
pub struct HopBucket {
    pub hops: usize,
    pub count: usize,
}

/// Full stats payload served by `GET /api/stats`.
#[derive(Serialize, Clone, Default)]
pub struct GraphStats {
    pub num_nodes: u64,
    pub num_edges: u64,
    pub top_in_degree: Vec<PageStat>,
    pub top_out_degree: Vec<PageStat>,
    pub hop_distribution: Vec<HopBucket>,
}

/// Number of entries kept per "top pages" list.
const TOP_N: usize = 20;

/// Compute the full stats payload.
///
/// Degree stats are an O(V) scan of the offset arrays. The hop distribution
/// runs a handful of seeded single-source BFS (depth-limited) from popular
/// hub nodes and buckets the reached nodes by distance.
pub fn compute(graph: &LoadedGraph, titles: &[String]) -> GraphStats {
    let num_nodes = node_count(&graph.forward);
    let num_edges = edge_count(&graph.forward);

    let top_in_degree = top_degree(&graph.backward, titles, TOP_N);
    let top_out_degree = top_degree(&graph.forward, titles, TOP_N);

    let hop_distribution = hop_distribution(graph, &top_in_degree);

    GraphStats {
        num_nodes,
        num_edges,
        top_in_degree,
        top_out_degree,
        hop_distribution,
    }
}

fn node_count(csr: &WikiCsr) -> u64 {
    // offsets array is u64 entries; length is V+1
    (csr.offset_len() / 8).saturating_sub(1) as u64
}

fn edge_count(csr: &WikiCsr) -> u64 {
    csr.column_len() as u64 / 4
}

/// Top-N pages by degree in the given CSR (forward = out-degree, backward =
/// in-degree). Skips cids whose title is missing (deleted/non-article pages).
fn top_degree(csr: &WikiCsr, titles: &[String], n: usize) -> Vec<PageStat> {
    let v = (csr.offset_len() / 8).saturating_sub(1) as u32;

    // Track the current top-N in a small min-heap keyed by degree.
    let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, u32)>> =
        std::collections::BinaryHeap::with_capacity(n + 1);

    for cid in 0..v {
        let deg = csr.neighbors(cid).len();
        if deg == 0 {
            continue;
        }
        if heap.len() < n {
            heap.push(std::cmp::Reverse((deg, cid)));
        } else if let Some(&std::cmp::Reverse((min_deg, _))) = heap.peek() {
            if deg > min_deg {
                heap.pop();
                heap.push(std::cmp::Reverse((deg, cid)));
            }
        }
    }

    let mut out: Vec<PageStat> = heap
        .into_iter()
        .map(|std::cmp::Reverse((deg, cid))| PageStat {
            title: titles
                .get(cid as usize)
                .cloned()
                .unwrap_or_else(|| format!("#{}", cid)),
            cid,
            degree: deg,
        })
        .collect();
    out.sort_by_key(|p| std::cmp::Reverse(p.degree));
    out
}

/// Hop-distance histogram: run bounded single-source BFS from a handful of
/// high-degree hub nodes (taken from the in-degree top list) and bucket the
/// number of reached nodes per hop distance. This gives a sense of how quickly
/// the graph "fans out" from central articles.
///
/// Wikipedia's average out-degree is ~90, so an unbounded depth-5 BFS would
/// touch 90^5 ≈ 6 billion nodes. We cap both the depth (3) and the total nodes
/// visited per seed (200k) to keep this snappy while still being representative.
fn hop_distribution(graph: &LoadedGraph, seeds: &[PageStat]) -> Vec<HopBucket> {
    const SEED_COUNT: usize = 8;
    const MAX_DEPTH: usize = 3;
    const MAX_VISITED_PER_SEED: usize = 200_000;

    let mut buckets: Vec<u64> = vec![0; MAX_DEPTH + 1];

    let v = node_count(&graph.forward) as usize;
    let mut visited = vec![false; v];

    for seed in seeds.iter().take(SEED_COUNT) {
        let mut touched: Vec<u32> = Vec::new();

        let mut q: VecDeque<(u32, usize)> = VecDeque::new();
        visited[seed.cid as usize] = true;
        touched.push(seed.cid);
        q.push_back((seed.cid, 0));

        while let Some((node, depth)) = q.pop_front() {
            if depth >= MAX_DEPTH || touched.len() >= MAX_VISITED_PER_SEED {
                continue;
            }
            for &nb in graph.forward.neighbors(node) {
                let idx = nb as usize;
                if idx < visited.len() && !visited[idx] {
                    visited[idx] = true;
                    touched.push(nb);
                    buckets[depth + 1] += 1;
                    q.push_back((nb, depth + 1));
                    if touched.len() >= MAX_VISITED_PER_SEED {
                        break;
                    }
                }
            }
        }

        // clear only the nodes we touched, so the visited array is fresh for
        // the next seed without re-allocating ~7M entries each time.
        for cid in touched {
            visited[cid as usize] = false;
        }
    }

    buckets
        .into_iter()
        .enumerate()
        .map(|(hops, count)| HopBucket {
            hops,
            count: count as usize,
        })
        .collect()
}

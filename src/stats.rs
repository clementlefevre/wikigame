/// Graph analytics: degree-based "top pages" stats, degree distribution,
/// dead-end / orphan counts, a hop-distance distribution histogram,
/// a random-pair degrees-of-separation histogram, global PageRank, and the
/// "first-link chain" helper used by the Road-to-Philosophy visualizer.
///
/// Everything here runs on the memory-mapped CSR graphs and is O(V + E) or
/// cheaper (PageRank is O(iters * E)). Results are serialised by `web.rs`
/// and cached inside `AppState` after the first request (or precomputed
/// right after graph load).
use std::collections::VecDeque;

use serde::Serialize;

use crate::graph::{LoadedGraph, WikiCsr};
use crate::search::shortest_path;

/// A single ranked page entry.
#[derive(Serialize, Clone)]
pub struct PageStat {
    pub title: String,
    pub cid: u32,
    pub degree: usize,
}

/// A ranked PageRank entry (uses `score` instead of `degree`).
#[derive(Serialize, Clone)]
pub struct PageRankStat {
    pub title: String,
    pub cid: u32,
    pub score: f32,
}

/// Bucket of the hop-distance histogram.
#[derive(Serialize, Clone)]
pub struct HopBucket {
    pub hops: usize,
    pub count: usize,
}

/// Bucket of the degree-distribution histogram (power-law buckets).
#[derive(Serialize, Clone)]
pub struct DegreeBucket {
    pub range: String,
    pub count: u64,
}

/// Full stats payload served by `GET /api/stats`.
#[derive(Serialize, Clone, Default)]
pub struct GraphStats {
    pub num_nodes: u64,
    pub num_edges: u64,
    pub density: f64,
    pub avg_degree: f64,
    pub dead_ends: u64,
    pub orphans: u64,
    pub self_loops: u64,
    pub degree_distribution: Vec<DegreeBucket>,
    pub top_in_degree: Vec<PageStat>,
    pub top_out_degree: Vec<PageStat>,
    pub top_dead_ends: Vec<PageStat>,
    pub hop_distribution: Vec<HopBucket>,
    /// Random-pair shortest-path length histogram (the "six degrees of
    /// Wikipedia" chart). Buckets are path lengths 0..=MAX_SEP; the final
    /// bucket with `hops == MAX_SEP_HOPS+1` (sentinel) counts pairs with no
    /// path found within the depth cap.
    pub separation_distribution: Vec<HopBucket>,
    pub top_pagerank: Vec<PageRankStat>,
}

/// Number of entries kept per "top pages" list.
const TOP_N: usize = 20;

/// Maximum path length we record in the separation histogram. Longer paths
/// (or pairs that fail to meet within the cap) roll into the "no path" bucket.
pub const MAX_SEP_HOPS: usize = 8;

/// Number of random pairs sampled for the separation histogram. Each pair is
/// a bidirectional BFS that's cheap (d≈4 on Wikipedia), but on the full
/// 648M-edge graph each BFS still touches millions of nodes, so we keep
/// this modest.
const SEP_SAMPLES: usize = 200;

/// PageRank damping factor and iteration count.
const PR_DAMPING: f64 = 0.85;
const PR_ITERS: usize = 20;

/// Compute the fast stats payload (everything except PageRank and the
/// separation distribution). This is O(V+E) and completes in a few seconds
/// even on the full 648M-edge graph.
pub fn compute_fast(graph: &LoadedGraph, titles: &[String]) -> GraphStats {
    let num_nodes = node_count(&graph.forward);
    let num_edges = edge_count(&graph.forward);

    let density = if num_nodes > 1 {
        num_edges as f64 / (num_nodes as f64 * (num_nodes as f64 - 1.0))
    } else {
        0.0
    };
    let avg_degree = if num_nodes > 0 {
        num_edges as f64 / num_nodes as f64
    } else {
        0.0
    };

    // Single O(V) pass: degree stats + structural metrics + top-N heaps.
    let (dead_ends, orphans, self_loops, degree_distribution, top_dead_ends, top_in_degree, top_out_degree) =
        structural_stats_and_degrees(graph, titles);

    let hop_distribution = hop_distribution(graph, &top_in_degree);

    GraphStats {
        num_nodes,
        num_edges,
        density,
        avg_degree,
        dead_ends,
        orphans,
        self_loops,
        degree_distribution,
        top_in_degree,
        top_out_degree,
        top_dead_ends,
        hop_distribution,
        separation_distribution: Vec::new(),
        top_pagerank: Vec::new(),
    }
}

/// Compute the separation distribution (random-pair BFS histogram). This is
/// expensive on the full graph (each BFS touches millions of nodes) so it's
/// computed lazily via a separate endpoint.
pub fn compute_separation(graph: &LoadedGraph) -> Vec<HopBucket> {
    separation_distribution(graph)
}

/// Compute PageRank and return the top-N entries plus the full vector (for
/// caching). This is O(PR_ITERS * E) and takes several minutes on the full
/// graph.
pub fn compute_pagerank(graph: &LoadedGraph, titles: &[String]) -> (Vec<PageRankStat>, Vec<f32>) {
    let pr = pagerank(&graph.forward);
    let top = top_pagerank_from_slice(&pr, titles, TOP_N);
    (top, pr)
}

/// Compute the full stats payload.
///
/// If `pagerank_cache` is `Some`, the PageRank vector is reused instead of
/// being recomputed (the stats endpoint and the search endpoint both need
/// PageRank, so we share the work).
///
/// Returns the stats payload *and* the full per-node PageRank vector (so the
/// caller can cache it for the /search endpoint).
pub fn compute(
    graph: &LoadedGraph,
    titles: &[String],
    pagerank_cache: Option<&[f32]>,
) -> (GraphStats, Vec<f32>) {
    let mut stats = compute_fast(graph, titles);

    let pagerank: Vec<f32> = match pagerank_cache {
        Some(c) => c.to_vec(),
        None => pagerank(&graph.forward),
    };
    stats.top_pagerank = top_pagerank_from_slice(&pagerank, titles, TOP_N);

    (stats, pagerank)
}

fn node_count(csr: &WikiCsr) -> u64 {
    (csr.offset_len() / 8).saturating_sub(1) as u64
}

fn edge_count(csr: &WikiCsr) -> u64 {
    csr.column_len() as u64 / 4
}

/// Compute structural graph metrics in a single O(V) pass:
///   - dead_ends: nodes with 0 out-degree (sink / dead-end articles)
///   - orphans: nodes with 0 in-degree (nothing links to them)
///   - self_loops: edges where src == dst (articles linking to themselves)
///   - degree_distribution: power-law bucketed out-degree histogram
///   - top_dead_ends: highest in-degree among dead-end nodes
///
/// Also returns top-N by in-degree and out-degree (computed in the same pass
/// to avoid scanning all nodes multiple times).
fn structural_stats_and_degrees(
    graph: &LoadedGraph,
    titles: &[String],
) -> (
    u64,
    u64,
    u64,
    Vec<DegreeBucket>,
    Vec<PageStat>,
    Vec<PageStat>,
    Vec<PageStat>,
) {
    let v = node_count(&graph.forward) as usize;

    let mut dead_ends: u64 = 0;
    let mut orphans: u64 = 0;
    let mut self_loops: u64 = 0;

    let bucket_ranges = ["0", "1", "2–5", "6–10", "11–50", "51–100", "101–500", "500+"];
    let mut buckets = [0u64; 8];

    let mut dead_end_heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, u32)>> =
        std::collections::BinaryHeap::with_capacity(TOP_N + 1);
    let mut in_deg_heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, u32)>> =
        std::collections::BinaryHeap::with_capacity(TOP_N + 1);
    let mut out_deg_heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, u32)>> =
        std::collections::BinaryHeap::with_capacity(TOP_N + 1);

    for cid in 0..v as u32 {
        let out_deg = graph.forward.neighbors(cid).len();
        let in_deg = graph.backward.neighbors(cid).len();

        if out_deg == 0 {
            dead_ends += 1;
            if in_deg > 0 {
                if dead_end_heap.len() < TOP_N {
                    dead_end_heap.push(std::cmp::Reverse((in_deg, cid)));
                } else if let Some(&std::cmp::Reverse((min_deg, _))) = dead_end_heap.peek() {
                    if in_deg > min_deg {
                        dead_end_heap.pop();
                        dead_end_heap.push(std::cmp::Reverse((in_deg, cid)));
                    }
                }
            }
        }

        if in_deg == 0 {
            orphans += 1;
        }

        // Self-loop check: only scan if out_deg is small (self-loops are rare
        // and scanning all neighbors of high-degree nodes is expensive).
        if out_deg <= 50 {
            for &nb in graph.forward.neighbors(cid) {
                if nb == cid {
                    self_loops += 1;
                    break;
                }
            }
        }

        // Update top-N heaps.
        if out_deg > 0 {
            if out_deg_heap.len() < TOP_N {
                out_deg_heap.push(std::cmp::Reverse((out_deg, cid)));
            } else if let Some(&std::cmp::Reverse((min_deg, _))) = out_deg_heap.peek() {
                if out_deg > min_deg {
                    out_deg_heap.pop();
                    out_deg_heap.push(std::cmp::Reverse((out_deg, cid)));
                }
            }
        }
        if in_deg > 0 {
            if in_deg_heap.len() < TOP_N {
                in_deg_heap.push(std::cmp::Reverse((in_deg, cid)));
            } else if let Some(&std::cmp::Reverse((min_deg, _))) = in_deg_heap.peek() {
                if in_deg > min_deg {
                    in_deg_heap.pop();
                    in_deg_heap.push(std::cmp::Reverse((in_deg, cid)));
                }
            }
        }

        let bidx = match out_deg {
            0 => 0,
            1 => 1,
            2..=5 => 2,
            6..=10 => 3,
            11..=50 => 4,
            51..=100 => 5,
            101..=500 => 6,
            _ => 7,
        };
        buckets[bidx] += 1;
    }

    let degree_distribution = bucket_ranges
        .iter()
        .zip(buckets.iter())
        .map(|(range, &count)| DegreeBucket {
            range: range.to_string(),
            count,
        })
        .collect();

    let make_stats = |heap: std::collections::BinaryHeap<std::cmp::Reverse<(usize, u32)>>| {
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
    };

    let top_dead_ends = make_stats(dead_end_heap);
    let top_in_degree = make_stats(in_deg_heap);
    let top_out_degree = make_stats(out_deg_heap);

    (
        dead_ends,
        orphans,
        self_loops,
        degree_distribution,
        top_dead_ends,
        top_in_degree,
        top_out_degree,
    )
}

/// Hop-distance histogram: run bounded single-source BFS from a handful of
/// high-degree hub nodes (taken from the in-degree top list) and bucket the
/// number of reached nodes per hop distance.
fn hop_distribution(graph: &LoadedGraph, seeds: &[PageStat]) -> Vec<HopBucket> {
    const SEED_COUNT: usize = 4;
    const MAX_DEPTH: usize = 2;
    const MAX_VISITED_PER_SEED: usize = 50_000;

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

/// Degrees-of-separation histogram: sample `SEP_SAMPLES` random node pairs
/// and run a bidirectional BFS for each, bucketing the resulting path
/// lengths. Wikipedia's average path length is ~3–4 hops, so this confirms
/// the "small world" phenomenon on the real graph.
///
/// Pairs with no path (e.g. hitting dead-ends, or unreachable clusters)
/// are recorded under the sentinel bucket `MAX_SEP_HOPS + 1`.
fn separation_distribution(graph: &LoadedGraph) -> Vec<HopBucket> {
    use rand::seq::SliceRandom;

    let n_nodes = node_count(&graph.forward);
    if n_nodes < 2 {
        return Vec::new();
    }

    let mut rng = rand::thread_rng();
    let mut buckets: Vec<u64> = vec![0; MAX_SEP_HOPS + 2]; // 0..MAX, +1 "no path"
    let mut sampled = 0usize;

    // Build a list of non-dead-end, non-orphan "active" nodes to sample from.
    // Sampling purely random cids would hit many dead-ends and produce
    // uninteresting "no path" results. Instead we pick from nodes that have
    // both in- and out-degree > 0.
    let v = n_nodes as usize;
    let mut active: Vec<u32> = Vec::with_capacity(v.min(2_000_000));
    for cid in 0..v as u32 {
        if !graph.forward.neighbors(cid).is_empty()
            && !graph.backward.neighbors(cid).is_empty()
        {
            active.push(cid);
            if active.len() >= 2_000_000 {
                break;
            }
        }
    }
    if active.len() < 2 {
        return Vec::new();
    }

    while sampled < SEP_SAMPLES {
        let a = *active.choose(&mut rng).unwrap();
        let b = *active.choose(&mut rng).unwrap();
        if a == b {
            continue;
        }
        sampled += 1;

        match shortest_path(&graph.forward, &graph.backward, a, b) {
            Some(r) => {
                let h = r.hops.min(MAX_SEP_HOPS + 1);
                buckets[h] += 1;
            }
            None => {
                buckets[MAX_SEP_HOPS + 1] += 1;
            }
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

/// Compute PageRank for every node using power iteration on the forward CSR.
///
/// Standard formula:  PR(A) = (1-d) + d * Σ PR(T_i)/C(T_i)
/// where the sum is over pages T_i that link to A, and C(T_i) is T_i's
/// out-degree. We iterate on the *backward* CSR (in-edges) so each node's
/// update is a simple gather of its backward neighbours' contributions.
///
/// Returns one f32 per node (rank sum ≈ N before normalisation).
pub fn pagerank(forward: &WikiCsr) -> Vec<f32> {
    let n = (forward.offset_len() / 8).saturating_sub(1);
    if n == 0 {
        return Vec::new();
    }

    // Precompute out-degree per node (we need it as f64 in the inner loop).
    let out_deg: Vec<u32> = (0..n as u32).map(|cid| forward.neighbors(cid).len() as u32).collect();

    // We need the *backward* graph to gather incoming contributions per node.
    // The forward CSR has columns[off..off+deg] = out-neighbours, so we
    // build a backward adjacency on the fly? That would require the backward
    // CSR — but `pagerank` is called with only the forward CSR for the
    // public API. We instead scatter during iteration: for each node n, push
    // its rank/outdeg to each of its out-neighbours. This is O(E) per iter
    // and avoids needing the backward CSR.
    let mut cur: Vec<f64> = vec![1.0; n];
    let mut next: Vec<f64> = vec![0.0; n];

    let base = (1.0 - PR_DAMPING) * (n as f64); // each node's teleport share

    for _ in 0..PR_ITERS {
        // Reset next to the teleport term so dangling nodes still pass rank.
        for v in next.iter_mut() {
            *v = base;
        }

        // Compute total rank held by dangling nodes (out-degree 0). Their rank
        // is redistributed uniformly to all nodes (standard PageRank fixup).
        let mut dangling = 0.0f64;
        for cid in 0..n {
            if out_deg[cid] == 0 {
                dangling += cur[cid];
            }
        }
        let dangling_share = PR_DAMPING * dangling / (n as f64);
        if dangling_share > 0.0 {
            for v in next.iter_mut() {
                *v += dangling_share;
            }
        }

        // Scatter: for each node, distribute d * PR / out_deg to each out-neighbor.
        for cid in 0..n as u32 {
            let deg = out_deg[cid as usize];
            if deg == 0 {
                continue;
            }
            let share = PR_DAMPING * cur[cid as usize] / (deg as f64);
            for &nb in forward.neighbors(cid) {
                let idx = nb as usize;
                if idx < n {
                    next[idx] += share;
                }
            }
        }

        std::mem::swap(&mut cur, &mut next);
    }

    // Normalise so ranks sum to 1.0 for display convenience.
    let total: f64 = cur.iter().sum();
    let norm = if total > 0.0 { 1.0 / total } else { 1.0 };
    cur.iter().map(|&v| (v * norm) as f32).collect()
}

/// Top-N pages by PageRank score.
pub fn top_pagerank_from_slice(ranks: &[f32], titles: &[String], n: usize) -> Vec<PageRankStat> {
    // f32 isn't Ord, so we collect (bits_as_u32, cid) pairs and use the
    // bit pattern for ordering (works correctly for non-negative f32 values,
    // which PageRank always is).
    let mut indexed: Vec<(u32, u32)> = ranks
        .iter()
        .enumerate()
        .map(|(cid, &score)| (score.to_bits(), cid as u32))
        .collect();

    // Partial sort so we only pay for the top-N. uses the bit-ordering of
    // IEEE-754 floats (correct for non-negative values).
    indexed.sort_by_key(|&(b, _)| std::cmp::Reverse(b));

    indexed
        .into_iter()
        .take(n)
        .map(|(bits, cid)| {
            let score = f32::from_bits(bits);
            PageRankStat {
                title: titles
                    .get(cid as usize)
                    .cloned()
                    .unwrap_or_else(|| format!("#{}", cid)),
                cid,
                score,
            }
        })
        .collect()
}

// ── Road to Philosophy: first-link chain follower ─────────────────────────────

/// Follow the "first link" of each article in sequence.
///
/// Wikipedia's well-known quirk: clicking the first (non-parenthesised,
/// non-italicised) link on almost any article eventually reaches
/// **Philosophy**. The dump doesn't preserve link ordering faithfully, so
/// we approximate "first link" as the lexicographically smallest outgoing
/// neighbour title. This is deterministic and cheap and matches the spirit
/// of the phenomenon well enough for a visualisation.
///
/// Stops when:
///   - we reach `Philosophy` (or `target` if provided),
///   - we hit a cycle (a node we've already visited),
///   - we hit a dead-end (no outgoing links),
///   - or `max_steps` is reached.
pub fn first_link_chain(
    forward: &WikiCsr,
    titles: &[String],
    start_cid: u32,
    target: &str,
    max_steps: usize,
) -> FirstLinkChain {
    let mut chain: Vec<u32> = vec![start_cid];
    let mut visited: hashbrown::HashSet<u32> = hashbrown::HashSet::new();
    visited.insert(start_cid);

    let target_key = target.replace(' ', "_");
    let mut status = FirstLinkStatus::MaxSteps;

    for _ in 0..max_steps {
        let cur = *chain.last().unwrap();
        let next = match first_link_of(forward, titles, cur) {
            Some(n) => n,
            None => {
                status = FirstLinkStatus::DeadEnd;
                break;
            }
        };

        if titles.get(next as usize).map(|t| t.as_str()) == Some(target_key.as_str()) {
            chain.push(next);
            status = FirstLinkStatus::Reached;
            break;
        }

        if visited.contains(&next) {
            chain.push(next);
            status = FirstLinkStatus::Cycle;
            break;
        }
        visited.insert(next);
        chain.push(next);
    }

    let chain_titles: Vec<String> = chain
        .iter()
        .map(|&cid| {
            titles
                .get(cid as usize)
                .cloned()
                .unwrap_or_else(|| format!("#{}", cid))
        })
        .collect();

    FirstLinkChain {
        chain: chain_titles,
        status,
    }
}

/// Pick the lexicographically smallest outgoing neighbour title as the
/// "first link" proxy. Returns None if the page has no outgoing links.
/// We resolve titles via the `titles` table; cids with no resolvable title
/// are skipped (they're almost always non-article namespaces).
fn first_link_of(forward: &WikiCsr, titles: &[String], cid: u32) -> Option<u32> {
    let neighbors = forward.neighbors(cid);
    if neighbors.is_empty() {
        return None;
    }

    let mut best: Option<(String, u32)> = None;
    for &nb in neighbors {
        if let Some(t) = titles.get(nb as usize) {
            // Skip empty / placeholder titles.
            if t.is_empty() {
                continue;
            }
            match &best {
                None => best = Some((t.clone(), nb)),
                Some((b, _)) => {
                    if t.as_str() < b.as_str() {
                        best = Some((t.clone(), nb));
                    }
                }
            }
        }
    }
    best.map(|(_, nb)| nb)
}

/// Termination status of a first-link chain walk.
#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FirstLinkStatus {
    Reached,
    Cycle,
    DeadEnd,
    MaxSteps,
}

/// Result of a first-link chain walk, served by `GET /api/first-link`.
#[derive(Serialize, Clone)]
pub struct FirstLinkChain {
    pub chain: Vec<String>,
    pub status: FirstLinkStatus,
}

// ── Ego network ──────────────────────────────────────────────────────────────

/// An ego network: a focal node ("ego") plus its 1- or 2-hop neighbourhood.
#[derive(Serialize, Clone)]
pub struct EgoNetwork {
    pub ego: String,
    pub ego_cid: u32,
    pub hops: u8,
    pub nodes: Vec<EgoNode>,
    /// Edges as pairs of cids (both endpoints are present in `nodes`).
    pub edges: Vec<(u32, u32)>,
}

#[derive(Serialize, Clone)]
pub struct EgoNode {
    pub title: String,
    pub cid: u32,
    pub degree: usize,
    /// Hop distance from the ego (1 or 2).
    pub hop: u8,
}

/// Compute the local 1- or 2-hop ego network around `ego_cid`. The number of
/// returned nodes is capped at `limit` (the ego itself is always included).
/// Edges are only those where *both* endpoints are in the returned node set.
pub fn ego_network(
    graph: &LoadedGraph,
    titles: &[String],
    ego_cid: u32,
    hops: u8,
    limit: usize,
) -> EgoNetwork {
    let limit = limit.clamp(1, 500);
    let hops = hops.clamp(1, 2);

    let title_of = |cid: u32| {
        titles
            .get(cid as usize)
            .cloned()
            .unwrap_or_else(|| format!("#{}", cid))
    };

    let mut included: hashbrown::HashMap<u32, u8> = hashbrown::HashMap::new();
    included.insert(ego_cid, 0);

    // BFS up to `hops`, capped at `limit` nodes total.
    let mut frontier: Vec<u32> = vec![ego_cid];
    for hop in 1..=hops {
        if included.len() >= limit {
            break;
        }
        let mut next_frontier: Vec<u32> = Vec::new();
        for &node in &frontier {
            for &nb in graph.forward.neighbors(node) {
                if !included.contains_key(&nb) {
                    included.insert(nb, hop);
                    next_frontier.push(nb);
                    if included.len() >= limit {
                        break;
                    }
                }
            }
            if included.len() >= limit {
                break;
            }
        }
        frontier = next_frontier;
    }

    // Build node list.
    let mut nodes: Vec<EgoNode> = Vec::with_capacity(included.len());
    for (&cid, &hop) in included.iter() {
        nodes.push(EgoNode {
            title: title_of(cid),
            cid,
            degree: graph.forward.neighbors(cid).len(),
            hop,
        });
    }
    nodes.sort_by_key(|n| (n.hop, std::cmp::Reverse(n.degree)));

    // Build edges: only forward edges where both endpoints are included.
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for &cid in included.keys() {
        for &nb in graph.forward.neighbors(cid) {
            if included.contains_key(&nb) && nb != cid {
                edges.push((cid, nb));
            }
        }
    }
    edges.sort();
    edges.dedup();

    EgoNetwork {
        ego: title_of(ego_cid),
        ego_cid,
        hops,
        nodes,
        edges,
    }
}

// ── BFS frontier trace ───────────────────────────────────────────────────────

/// One layer of the bidirectional BFS frontier expansion.
#[derive(Serialize, Clone)]
pub struct BfsLayer {
    pub depth: usize,
    /// "fwd" = expanded from start going forward; "bwd" = expanded from end going backward.
    pub direction: String,
    pub nodes: Vec<String>,
    pub count: usize,
}

/// Full trace of a bidirectional BFS, served by `POST /api/bfs-trace`.
#[derive(Serialize, Clone)]
pub struct BfsTrace {
    pub from: String,
    pub to: String,
    pub layers: Vec<BfsLayer>,
    pub meeting_node: Option<String>,
    pub path: Vec<String>,
    pub total_expanded: usize,
}

/// Run a bidirectional BFS that records every frontier layer, for the
/// animated "BFS frontier expansion" visualisation. Caps each direction's
/// total expansion at `max_nodes` to keep payloads reasonable.
pub fn bfs_trace(
    graph: &LoadedGraph,
    titles: &[String],
    start_cid: u32,
    end_cid: u32,
    max_nodes: usize,
) -> BfsTrace {
    let title_of = |cid: u32| {
        titles
            .get(cid as usize)
            .cloned()
            .unwrap_or_else(|| format!("#{}", cid))
    };

    let mut layers: Vec<BfsLayer> = Vec::new();

    if start_cid == end_cid {
        return BfsTrace {
            from: title_of(start_cid),
            to: title_of(end_cid),
            layers,
            meeting_node: Some(title_of(start_cid)),
            path: vec![title_of(start_cid)],
            total_expanded: 1,
        };
    }

    let mut visited_fwd: hashbrown::HashMap<u32, u32> = hashbrown::HashMap::new();
    let mut visited_bwd: hashbrown::HashMap<u32, u32> = hashbrown::HashMap::new();
    visited_fwd.insert(start_cid, start_cid);
    visited_bwd.insert(end_cid, end_cid);

    let mut q_fwd: VecDeque<u32> = VecDeque::new();
    let mut q_bwd: VecDeque<u32> = VecDeque::new();
    q_fwd.push_back(start_cid);
    q_bwd.push_back(end_cid);

    // Record the seed layers (depth 0).
    layers.push(BfsLayer {
        depth: 0,
        direction: "fwd".into(),
        nodes: vec![title_of(start_cid)],
        count: 1,
    });
    layers.push(BfsLayer {
        depth: 0,
        direction: "bwd".into(),
        nodes: vec![title_of(end_cid)],
        count: 1,
    });

    let mut total_expanded = 2usize;
    let mut meeting_node: Option<u32> = None;
    let mut depth_fwd: usize = 0;
    let mut depth_bwd: usize = 0;

    while (!q_fwd.is_empty() || !q_bwd.is_empty()) && total_expanded < max_nodes {
        let expand_forward = match (q_fwd.is_empty(), q_bwd.is_empty()) {
            (true, _) => false,
            (_, true) => true,
            _ => q_fwd.len() <= q_bwd.len(),
        };

        if expand_forward {
            depth_fwd += 1;
            let layer_size = q_fwd.len();
            let mut new_layer: Vec<u32> = Vec::new();
            for _ in 0..layer_size {
                let node = match q_fwd.pop_front() {
                    Some(n) => n,
                    None => break,
                };
                for &nb in graph.forward.neighbors(node) {
                    if !visited_fwd.contains_key(&nb) {
                        visited_fwd.insert(nb, node);
                        q_fwd.push_back(nb);
                        new_layer.push(nb);
                        total_expanded += 1;
                        if visited_bwd.contains_key(&nb) {
                            meeting_node = Some(nb);
                        }
                        if total_expanded >= max_nodes {
                            break;
                        }
                    }
                }
                if meeting_node.is_some() || total_expanded >= max_nodes {
                    break;
                }
            }
            if !new_layer.is_empty() {
                let nodes: Vec<String> = new_layer.iter().map(|&c| title_of(c)).collect();
                let count = nodes.len();
                layers.push(BfsLayer {
                    depth: depth_fwd,
                    direction: "fwd".into(),
                    nodes,
                    count,
                });
            }
        } else {
            depth_bwd += 1;
            let layer_size = q_bwd.len();
            let mut new_layer: Vec<u32> = Vec::new();
            for _ in 0..layer_size {
                let node = match q_bwd.pop_front() {
                    Some(n) => n,
                    None => break,
                };
                for &nb in graph.backward.neighbors(node) {
                    if !visited_bwd.contains_key(&nb) {
                        visited_bwd.insert(nb, node);
                        q_bwd.push_back(nb);
                        new_layer.push(nb);
                        total_expanded += 1;
                        if visited_fwd.contains_key(&nb) {
                            meeting_node = Some(nb);
                        }
                        if total_expanded >= max_nodes {
                            break;
                        }
                    }
                }
                if meeting_node.is_some() || total_expanded >= max_nodes {
                    break;
                }
            }
            if !new_layer.is_empty() {
                let nodes: Vec<String> = new_layer.iter().map(|&c| title_of(c)).collect();
                let count = nodes.len();
                layers.push(BfsLayer {
                    depth: depth_bwd,
                    direction: "bwd".into(),
                    nodes,
                    count,
                });
            }
        }

        if meeting_node.is_some() {
            break;
        }
    }

    // Reconstruct the path from the visited maps if we found a meeting node.
    let path: Vec<String> = match meeting_node {
        Some(m) => {
            let mut fwd_path: Vec<u32> = Vec::new();
            let mut cur = m;
            loop {
                fwd_path.push(cur);
                let parent = visited_fwd[&cur];
                if parent == cur {
                    break;
                }
                cur = parent;
            }
            fwd_path.reverse();

            let mut bwd_path: Vec<u32> = Vec::new();
            let mut cur = m;
            loop {
                let parent = visited_bwd[&cur];
                if parent == cur {
                    break;
                }
                cur = parent;
                bwd_path.push(cur);
            }
            bwd_path.reverse();

            let mut full = fwd_path;
            full.extend(bwd_path);
            full.iter().map(|&c| title_of(c)).collect()
        }
        None => Vec::new(),
    };

    BfsTrace {
        from: title_of(start_cid),
        to: title_of(end_cid),
        layers,
        meeting_node: meeting_node.map(title_of),
        path,
        total_expanded,
    }
}

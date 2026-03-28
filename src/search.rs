/// Bidirectional BFS shortest-path search on the Wikipedia link graph.
///
/// Algorithm:
///    Two simultaneous BFS frontiers — one from `start` (forward on the link
///    graph) and one from `end` (forward on the *transposed* graph, i.e.
///    backward links).  On each step we expand the smaller frontier.
///    The search terminates as soon as a node appears in both visited sets.
///
/// Complexity:
///    O(b^(d/2)) vs O(b^d) for unidirectional BFS, where b is the average
///    branching factor (~140 out-links per article) and d is the diameter.
///    In practice: d≈4 for Wikipedia, so bi-BFS explores ~√(140^4) ≈ 20,000
///    nodes instead of 140^4 ≈ 384 million.
use std::collections::VecDeque;
use std::time::Instant;

use hashbrown::HashMap;

use crate::graph::WikiGraph;
use petgraph::visit::NodeIndexable;

#[derive(Debug)]
pub struct SearchResult {
    /// Ordered list of compact IDs from start to end (inclusive).
    pub path: Vec<u32>,
    /// Number of hops = path.len() - 1.
    pub hops: usize,
    /// Wall-clock time for the search (excludes graph loading).
    pub elapsed_ms: u64,
}

/// Find the shortest path between `start_cid` and `end_cid`.
/// Returns `None` if the nodes are not connected.
pub fn shortest_path(
    forward: &WikiGraph,
    backward: &WikiGraph,
    start_cid: u32,
    end_cid: u32,
) -> Option<SearchResult> {
    let t0 = Instant::now();

    if start_cid == end_cid {
        return Some(SearchResult {
            path: vec![start_cid],
            hops: 0,
            elapsed_ms: 0,
        });
    }

    // parent maps: node → the node we came from
    // visited_fwd: reached from start going forward
    // visited_bwd: reached from end going backward (i.e. via backward graph)
    let mut visited_fwd: HashMap<u32, u32> = HashMap::new();
    let mut visited_bwd: HashMap<u32, u32> = HashMap::new();

    let mut queue_fwd: VecDeque<u32> = VecDeque::new();
    let mut queue_bwd: VecDeque<u32> = VecDeque::new();

    // Sentinel: a node with itself as parent means it's a root.
    visited_fwd.insert(start_cid, start_cid);
    visited_bwd.insert(end_cid, end_cid);
    queue_fwd.push_back(start_cid);
    queue_bwd.push_back(end_cid);

    // Track which layer we're expanding so we can do entire-layer expansions.
    // This ensures we find the *shortest* path and not just *a* path.
    let mut meeting_node: Option<u32> = None;

    while !queue_fwd.is_empty() || !queue_bwd.is_empty() {
        // Expand the smaller frontier for efficiency.
        let expand_forward = match (queue_fwd.is_empty(), queue_bwd.is_empty()) {
            (true, _) => false,
            (_, true) => true,
            _ => queue_fwd.len() <= queue_bwd.len(),
        };

        if expand_forward {
            // Expand one full BFS layer of the forward frontier.
            let layer_size = queue_fwd.len();
            for _ in 0..layer_size {
                let node = match queue_fwd.pop_front() {
                    Some(n) => n,
                    None => break,
                };

                let node_idx = forward.from_index(node as usize);
                for &nb_idx in forward.neighbors_slice(node_idx) {
                    let nb = forward.to_index(nb_idx) as u32;
                    if !visited_fwd.contains_key(&nb) {
                        visited_fwd.insert(nb, node);
                        queue_fwd.push_back(nb);

                        if visited_bwd.contains_key(&nb) {
                            meeting_node = Some(nb);
                            break;
                        }
                    }
                }
                if meeting_node.is_some() {
                    break;
                }
            }
        } else {
            // Expand one full BFS layer of the backward frontier.
            let layer_size = queue_bwd.len();
            for _ in 0..layer_size {
                let node = match queue_bwd.pop_front() {
                    Some(n) => n,
                    None => break,
                };

                let node_idx = backward.from_index(node as usize);
                for &nb_idx in backward.neighbors_slice(node_idx) {
                    let nb = backward.to_index(nb_idx) as u32;
                    if !visited_bwd.contains_key(&nb) {
                        visited_bwd.insert(nb, node);
                        queue_bwd.push_back(nb);

                        if visited_fwd.contains_key(&nb) {
                            meeting_node = Some(nb);
                            break;
                        }
                    }
                }
                if meeting_node.is_some() {
                    break;
                }
            }
        }

        if meeting_node.is_some() {
            break;
        }
    }

    let meeting = meeting_node?;

    // Reconstruct path from start → meeting (via forward parents)
    let mut fwd_path: Vec<u32> = Vec::new();
    let mut cur = meeting;
    loop {
        fwd_path.push(cur);
        let parent = visited_fwd[&cur];
        if parent == cur {
            break; // reached the root (start node)
        }
        cur = parent;
    }
    fwd_path.reverse();

    // Reconstruct path from meeting → end (via backward parents, reversed)
    let mut bwd_path: Vec<u32> = Vec::new();
    let mut cur = meeting;
    loop {
        let parent = visited_bwd[&cur];
        if parent == cur {
            break; // reached the root (end node)
        }
        cur = parent;
        bwd_path.push(cur);
    }
    // bwd_path goes: meeting's successor in bwd direction → … → end
    // So the forward direction is: meeting → bwd_path reversed

    let mut path = fwd_path;
    path.extend(bwd_path.into_iter().rev());

    let hops = path.len().saturating_sub(1);
    let elapsed_ms = t0.elapsed().as_millis() as u64;

    Some(SearchResult {
        path,
        hops,
        elapsed_ms,
    })
}

/// Axum web server with a setup wizard.
///
/// Lifecycle:
///   GET  /                → serves the embedded HTML UI
///   GET  /api/status      → { state: "needs_setup" | "building" | "ready" | "error", message? }
///   POST /api/setup       → starts download + build in background (409 if already running)
///   GET  /api/progress    → SSE stream of ProgressEvents
///   POST /search          → JSON {from, to}  (503 unless ready)
///   POST /neighbors       → JSON {title, limit}  (503 unless ready)
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::{
    build_cmd::TitleIndex,
    graph::LoadedGraph,
    search::shortest_path,
    setup::{AppHandle, AppState},
    stats::{self, GraphStats},
};

struct WebState {
    handle: Arc<AppHandle>,
}

#[derive(Deserialize)]
struct SearchRequest {
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct NeighborsRequest {
    title: String,
    #[serde(default = "default_neighbor_limit")]
    limit: usize,
}

#[derive(Deserialize)]
struct EgoRequest {
    title: String,
    #[serde(default = "default_ego_hops")]
    hops: u8,
    #[serde(default = "default_ego_limit")]
    limit: usize,
}

#[derive(Deserialize)]
struct BfsTraceRequest {
    from: String,
    to: String,
    #[serde(default = "default_bfs_max_nodes")]
    max_nodes: usize,
}

#[derive(Deserialize)]
struct FirstLinkRequest {
    title: String,
    #[serde(default = "default_first_link_target")]
    target: String,
    #[serde(default = "default_first_link_max_steps")]
    max_steps: usize,
}

fn default_neighbor_limit() -> usize {
    40
}

fn default_ego_hops() -> u8 {
    1
}

fn default_ego_limit() -> usize {
    50
}

fn default_bfs_max_nodes() -> usize {
    5000
}

fn default_first_link_target() -> String {
    "Philosophy".to_string()
}

fn default_first_link_max_steps() -> usize {
    30
}

#[derive(Serialize)]
struct SearchResponse {
    path: Vec<String>,
    hops: usize,
    ms: u64,
    /// PageRank of each path node (normalised, sums to ~1 across the whole graph).
    #[serde(skip_serializing_if = "Option::is_none")]
    pagerank: Option<Vec<f32>>,
    /// In-degree of each path node.
    #[serde(skip_serializing_if = "Option::is_none")]
    path_degrees: Option<Vec<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct NeighborsResponse {
    title: String,
    total: usize,
    neighbors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct StatusResponse {
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn serve(port: u16, handle: AppHandle) {
    let handle = Arc::new(handle);
    let state = Arc::new(WebState {
        handle: handle.clone(),
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/setup", post(setup_handler))
        .route("/api/progress", get(progress_handler))
        .route("/api/stats", get(stats_handler))
        .route("/search", post(search_handler))
        .route("/neighbors", post(neighbors_handler))
        .route("/api/ego", post(ego_handler))
        .route("/api/bfs-trace", post(bfs_trace_handler))
        .route("/api/first-link", get(first_link_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index_handler() -> impl IntoResponse {
    Html(include_str!("../assets/index.html"))
}

async fn status_handler(State(ws): State<Arc<WebState>>) -> impl IntoResponse {
    let state = ws.handle.state.lock().await;
    let resp = match &*state {
        AppState::NeedsSetup => StatusResponse {
            state: "needs_setup".into(),
            message: None,
        },
        AppState::Building => StatusResponse {
            state: "building".into(),
            message: None,
        },
        AppState::Ready { .. } => StatusResponse {
            state: "ready".into(),
            message: None,
        },
        AppState::Error(msg) => StatusResponse {
            state: "error".into(),
            message: Some(msg.clone()),
        },
    };
    Json(resp)
}

async fn setup_handler(State(ws): State<Arc<WebState>>) -> impl IntoResponse {
    let mut state = ws.handle.state.lock().await;
    match &*state {
        AppState::Building => {
            return (StatusCode::CONFLICT, "Setup already running".to_string());
        }
        AppState::Ready { .. } => {
            return (StatusCode::OK, "Already ready".to_string());
        }
        _ => {}
    }
    // Transition to Building.
    *state = AppState::Building;

    let handle = ws.handle.clone();
    // Spawn the blocking setup on a dedicated thread.
    std::thread::spawn(move || {
        let downloads_dir = handle.downloads_dir.clone();
        let data_dir = handle.data_dir.clone();
        let reporter = handle.reporter.clone();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::setup::run_setup(&downloads_dir, &data_dir, &reporter);
        }));

        // We need a tokio runtime to acquire the async mutex + load graph.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime");
        rt.block_on(async {
            let mut state = handle.state.lock().await;
            match result {
                Ok(()) => match crate::setup::load_graph(&handle.data_dir) {
                    Some((graph, titles)) => {
                        *state = AppState::Ready {
                            graph,
                            titles,
                            stats: Arc::new(tokio::sync::Mutex::new(None)),
                            pagerank: Arc::new(tokio::sync::Mutex::new(None)),
                        };
                    }
                    None => {
                        *state = AppState::Error(
                            "Build reported success but graph files missing".into(),
                        );
                    }
                },
                Err(panic_payload) => {
                    let msg = panic_payload
                        .downcast_ref::<String>()
                        .map(|s| s.clone())
                        .or_else(|| panic_payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "Setup failed (panic)".to_string());
                    reporter.error(msg.clone());
                    *state = AppState::Error(msg);
                }
            }
        });
    });

    (StatusCode::ACCEPTED, "Setup started".to_string())
}

async fn progress_handler(State(ws): State<Arc<WebState>>) -> Response {
    let rx = ws.handle.reporter.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|r| r.ok())
        .map(|event| {
            let json = serde_json::to_string(&event).unwrap_or_default();
            Ok::<Event, Infallible>(Event::default().event(event.kind).data(json))
        });

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping"),
        )
        .into_response()
}

async fn search_handler(
    State(ws): State<Arc<WebState>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    let (graph, titles, pagerank_lock) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready {
                graph,
                titles,
                pagerank,
                ..
            } => (graph.clone(), titles.clone(), pagerank.clone()),
            _ => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(SearchResponse {
                        path: vec![],
                        hops: 0,
                        ms: 0,
                        pagerank: None,
                        path_degrees: None,
                        error: Some("Graph not ready yet.".into()),
                    }),
                )
                    .into_response();
            }
        }
    };

    // Fast path: pagerank already cached (computed by /api/stats).
    // We do NOT trigger pagerank computation from /search — it's a multi-minute
    // O(iters * E) job that would peg a CPU core and risk OOM. The /api/stats
    // endpoint is responsible for computing and caching it; until then, /search
    // returns paths without authority data (the `pagerank` field is omitted and
    // the UI handles its absence gracefully).
    let pr_cache: Option<Arc<Vec<f32>>> = {
        let guard = pagerank_lock.lock().await;
        guard.clone()
    };

    let path_task = tokio::task::spawn_blocking(move || {
        do_search(&graph, &titles, &req.from, &req.to)
    });

    let path_result = path_task
        .await
        .unwrap_or_else(|e| SearchResponse {
            path: vec![],
            hops: 0,
            ms: 0,
            pagerank: None,
            path_degrees: None,
            error: Some(format!("Internal error: {}", e)),
        });

    // If the search failed, return as-is.
    if path_result.error.is_some() {
        return Json(path_result).into_response();
    }

    // If pagerank isn't ready yet, return the path without authority data.
    // The UI handles a missing `pagerank` field gracefully.
    let ranks = match pr_cache {
        Some(c) => c,
        None => return Json(path_result).into_response(),
    };

    // Augment the response with per-node pagerank + in-degree.
    let (graph_for_deg, titles_for_lookup) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready { graph, titles, .. } => (graph.clone(), titles.clone()),
            _ => return Json(path_result).into_response(),
        }
    };

    let mut pr_vals: Vec<f32> = Vec::with_capacity(path_result.path.len());
    let mut deg_vals: Vec<usize> = Vec::with_capacity(path_result.path.len());
    for title in &path_result.path {
        let key = title.replace(' ', "_");
        let cid = titles_for_lookup.title_to_cid.get(&key).copied();
        if let Some(c) = cid {
            pr_vals.push(ranks.get(c as usize).copied().unwrap_or(0.0));
            deg_vals.push(graph_for_deg.backward.neighbors(c).len());
        } else {
            pr_vals.push(0.0);
            deg_vals.push(0);
        }
    }

    let mut response = path_result;
    response.pagerank = Some(pr_vals);
    response.path_degrees = Some(deg_vals);
    Json(response).into_response()
}

fn do_search(graph: &LoadedGraph, titles: &TitleIndex, from: &str, to: &str) -> SearchResponse {
    let from_key = from.replace(' ', "_");
    let to_key = to.replace(' ', "_");

    let start_cid = match titles.title_to_cid.get(&from_key) {
        Some(&c) => c,
        None => {
            return SearchResponse {
                path: vec![],
                hops: 0,
                ms: 0,
                pagerank: None,
                path_degrees: None,
                error: Some(format!("Article not found: \"{}\"", from)),
            };
        }
    };
    let end_cid = match titles.title_to_cid.get(&to_key) {
        Some(&c) => c,
        None => {
            return SearchResponse {
                path: vec![],
                hops: 0,
                ms: 0,
                pagerank: None,
                path_degrees: None,
                error: Some(format!("Article not found: \"{}\"", to)),
            };
        }
    };

    match shortest_path(&graph.forward, &graph.backward, start_cid, end_cid) {
        Some(result) => {
            let path_titles: Vec<String> = result
                .path
                .iter()
                .map(|&cid| {
                    titles
                        .titles
                        .get(cid as usize)
                        .cloned()
                        .unwrap_or_else(|| format!("#{}", cid))
                })
                .collect();
            SearchResponse {
                path: path_titles,
                hops: result.hops,
                ms: result.elapsed_ms,
                pagerank: None,
                path_degrees: None,
                error: None,
            }
        }
        None => SearchResponse {
            path: vec![],
            hops: 0,
            ms: 0,
            pagerank: None,
            path_degrees: None,
            error: Some("No path found between the two articles.".to_string()),
        },
    }
}

async fn neighbors_handler(
    State(ws): State<Arc<WebState>>,
    Json(req): Json<NeighborsRequest>,
) -> Response {
    let state = ws.handle.state.lock().await;
    let (graph, titles) = match &*state {
        AppState::Ready { graph, titles, .. } => (graph.clone(), titles.clone()),
        _ => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(NeighborsResponse {
                    title: req.title,
                    total: 0,
                    neighbors: vec![],
                    error: Some("Graph not ready yet.".into()),
                }),
            )
                .into_response();
        }
    };
    let title_for_resp = req.title.clone();
    let title_for_err = req.title.clone();
    let limit = req.limit;
    drop(state);
    let result =
        tokio::task::spawn_blocking(move || do_neighbors(&graph, &titles, &title_for_resp, limit))
            .await
            .unwrap_or_else(|e| NeighborsResponse {
                title: title_for_err,
                total: 0,
                neighbors: vec![],
                error: Some(format!("Internal error: {}", e)),
            });
    Json(result).into_response()
}
fn do_neighbors(
    graph: &LoadedGraph,
    titles: &TitleIndex,
    title: &str,
    limit: usize,
) -> NeighborsResponse {
    let key = title.replace(' ', "_");
    let cid = match titles.title_to_cid.get(&key) {
        Some(&c) => c,
        None => {
            return NeighborsResponse {
                title: title.to_string(),
                total: 0,
                neighbors: vec![],
                error: Some(format!("Article not found: \"{}\"", title)),
            };
        }
    };
    let neighbors_raw = graph.forward.neighbors(cid);
    let total = neighbors_raw.len();
    let limit = limit.max(1).min(500);
    let neighbors: Vec<String> = neighbors_raw
        .iter()
        .take(limit)
        .map(|&n| {
            titles
                .titles
                .get(n as usize)
                .cloned()
                .unwrap_or_else(|| format!("#{}", n))
        })
        .collect();
    NeighborsResponse {
        title: title.to_string(),
        total,
        neighbors,
        error: None,
    }
}

// ── Stats ────────────────────────────────────────────────────────────────────

async fn stats_handler(State(ws): State<Arc<WebState>>) -> Response {
    // Grab the graph + titles (and the stats cache lock) under the state mutex.
    let (graph, titles, stats_lock, pagerank_lock) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready {
                graph,
                titles,
                stats,
                pagerank,
                ..
            } => (graph.clone(), titles.clone(), stats.clone(), pagerank.clone()),
            _ => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({ "error": "Graph not ready yet." })),
                )
                    .into_response();
            }
        }
    };

    // Fast path: already computed.
    {
        let guard = stats_lock.lock().await;
        if let Some(cached) = guard.as_ref() {
            return Json((**cached).clone()).into_response();
        }
    }

    // If pagerank is already cached (e.g. computed by a prior /search call),
    // reuse it instead of recomputing inside stats::compute.
    let pr_cache: Option<Arc<Vec<f32>>> = {
        let guard = pagerank_lock.lock().await;
        guard.clone()
    };
    let pr_was_cached = pr_cache.is_some();

    // Slow path: compute on a blocking thread, then cache.
    let (computed, pr_vector) = tokio::task::spawn_blocking(move || {
        stats::compute(&graph, &titles.titles, pr_cache.as_ref().map(|v| v.as_slice()))
    })
    .await
    .unwrap_or_else(|_| (GraphStats::default(), Vec::new()));

    // Cache the full pagerank vector (if it was freshly computed) so the
    // /search endpoint can augment its responses with per-node authority.
    if !pr_was_cached && !pr_vector.is_empty() {
        let mut guard = pagerank_lock.lock().await;
        if guard.is_none() {
            *guard = Some(Arc::new(pr_vector));
        }
    }

    let cached = Arc::new(computed);

    let mut guard = stats_lock.lock().await;
    if guard.is_none() {
        *guard = Some(cached.clone());
    }
    Json((**guard.as_ref().unwrap()).clone()).into_response()
}

// ── Ego network ──────────────────────────────────────────────────────────────

async fn ego_handler(
    State(ws): State<Arc<WebState>>,
    Json(req): Json<EgoRequest>,
) -> Response {
    let (graph, titles) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready { graph, titles, .. } => (graph.clone(), titles.clone()),
            _ => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: "Graph not ready yet.".into(),
                    }),
                )
                    .into_response();
            }
        }
    };

    let title = req.title.clone();
    let hops = req.hops;
    let limit = req.limit;
    let result = tokio::task::spawn_blocking(move || {
        do_ego(&graph, &titles, &title, hops, limit)
    })
    .await;

    match result {
        Ok(Ok(ego)) => Json(ego).into_response(),
        Ok(Err(resp)) => *resp,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Internal error: {}", e),
            }),
        )
            .into_response(),
    }
}

fn do_ego(
    graph: &LoadedGraph,
    titles: &TitleIndex,
    title: &str,
    hops: u8,
    limit: usize,
) -> Result<stats::EgoNetwork, Box<Response>> {
    let key = title.replace(' ', "_");
    let cid = match titles.title_to_cid.get(&key) {
        Some(&c) => c,
        None => {
            return Err(Box::new(
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Article not found: \"{}\"", title),
                    }),
                )
                    .into_response(),
            ));
        }
    };
    Ok(stats::ego_network(graph, &titles.titles, cid, hops, limit))
}

// ── BFS frontier trace ───────────────────────────────────────────────────────

async fn bfs_trace_handler(
    State(ws): State<Arc<WebState>>,
    Json(req): Json<BfsTraceRequest>,
) -> Response {
    let (graph, titles) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready { graph, titles, .. } => (graph.clone(), titles.clone()),
            _ => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: "Graph not ready yet.".into(),
                    }),
                )
                    .into_response();
            }
        }
    };

    let from = req.from.clone();
    let to = req.to.clone();
    let max_nodes = req.max_nodes;
    let result = tokio::task::spawn_blocking(move || {
        do_bfs_trace(&graph, &titles, &from, &to, max_nodes)
    })
    .await;

    match result {
        Ok(Ok(trace)) => Json(trace).into_response(),
        Ok(Err(resp)) => *resp,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Internal error: {}", e),
            }),
        )
            .into_response(),
    }
}

fn do_bfs_trace(
    graph: &LoadedGraph,
    titles: &TitleIndex,
    from: &str,
    to: &str,
    max_nodes: usize,
) -> Result<stats::BfsTrace, Box<Response>> {
    let from_key = from.replace(' ', "_");
    let to_key = to.replace(' ', "_");
    let start_cid = match titles.title_to_cid.get(&from_key) {
        Some(&c) => c,
        None => {
            return Err(Box::new(
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Article not found: \"{}\"", from),
                    }),
                )
                    .into_response(),
            ));
        }
    };
    let end_cid = match titles.title_to_cid.get(&to_key) {
        Some(&c) => c,
        None => {
            return Err(Box::new(
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Article not found: \"{}\"", to),
                    }),
                )
                    .into_response(),
            ));
        }
    };
    Ok(stats::bfs_trace(
        graph,
        &titles.titles,
        start_cid,
        end_cid,
        max_nodes,
    ))
}

// ── First-link chain (Road to Philosophy) ────────────────────────────────────

async fn first_link_handler(
    State(ws): State<Arc<WebState>>,
    axum::extract::Query(req): axum::extract::Query<FirstLinkRequest>,
) -> Response {
    let (graph, titles) = {
        let state = ws.handle.state.lock().await;
        match &*state {
            AppState::Ready { graph, titles, .. } => (graph.clone(), titles.clone()),
            _ => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: "Graph not ready yet.".into(),
                    }),
                )
                    .into_response();
            }
        }
    };

    let title = req.title.clone();
    let target = req.target.clone();
    let max_steps = req.max_steps;
    let result = tokio::task::spawn_blocking(move || {
        do_first_link(&graph, &titles, &title, &target, max_steps)
    })
    .await;

    match result {
        Ok(Ok(chain)) => Json(chain).into_response(),
        Ok(Err(resp)) => *resp,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Internal error: {}", e),
            }),
        )
            .into_response(),
    }
}

fn do_first_link(
    graph: &LoadedGraph,
    titles: &TitleIndex,
    title: &str,
    target: &str,
    max_steps: usize,
) -> Result<stats::FirstLinkChain, Box<Response>> {
    let key = title.replace(' ', "_");
    let cid = match titles.title_to_cid.get(&key) {
        Some(&c) => c,
        None => {
            return Err(Box::new(
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("Article not found: \"{}\"", title),
                    }),
                )
                    .into_response(),
            ));
        }
    };
    Ok(stats::first_link_chain(
        &graph.forward,
        &titles.titles,
        cid,
        target,
        max_steps,
    ))
}

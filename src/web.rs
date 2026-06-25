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

fn default_neighbor_limit() -> usize {
    40
}

#[derive(Serialize)]
struct SearchResponse {
    path: Vec<String>,
    hops: usize,
    ms: u64,
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

pub async fn serve(port: u16, handle: AppHandle) {
    let handle = Arc::new(handle);
    let state = Arc::new(WebState { handle: handle.clone() });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/setup", post(setup_handler))
        .route("/api/progress", get(progress_handler))
        .route("/search", post(search_handler))
        .route("/neighbors", post(neighbors_handler))
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
        AppState::NeedsSetup => StatusResponse { state: "needs_setup".into(), message: None },
        AppState::Building => StatusResponse { state: "building".into(), message: None },
        AppState::Ready { .. } => StatusResponse { state: "ready".into(), message: None },
        AppState::Error(msg) => StatusResponse { state: "error".into(), message: Some(msg.clone()) },
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
                Ok(()) => {
                    match crate::setup::load_graph(&handle.data_dir) {
                        Some((graph, titles)) => {
                            *state = AppState::Ready { graph, titles };
                        }
                        None => {
                            *state = AppState::Error("Build reported success but graph files missing".into());
                        }
                    }
                }
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

async fn progress_handler(
    State(ws): State<Arc<WebState>>,
) -> Response {
    let rx = ws.handle.reporter.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| r.ok()).map(|event| {
        let json = serde_json::to_string(&event).unwrap_or_default();
        Ok::<Event, Infallible>(Event::default().event(event.kind).data(json))
    });

    Sse::new(stream).keep_alive(
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
    let state = ws.handle.state.lock().await;
    let (graph, titles) = match &*state {
        AppState::Ready { graph, titles } => (graph.clone(), titles.clone()),
        _ => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(SearchResponse {
                    path: vec![],
                    hops: 0,
                    ms: 0,
                    error: Some("Graph not ready yet.".into()),
                }),
            )
                .into_response();
        }
    };
    drop(state);
    let result = tokio::task::spawn_blocking(move || {
        do_search(&graph, &titles, &req.from, &req.to)
    })
    .await
    .unwrap_or_else(|e| SearchResponse {
        path: vec![],
        hops: 0,
        ms: 0,
        error: Some(format!("Internal error: {}", e)),
    });
    Json(result).into_response()
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
                error: None,
            }
        }
        None => SearchResponse {
            path: vec![],
            hops: 0,
            ms: 0,
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
        AppState::Ready { graph, titles } => (graph.clone(), titles.clone()),
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
    let result = tokio::task::spawn_blocking(move || {
        do_neighbors(&graph, &titles, &title_for_resp, limit)
    })
    .await
    .unwrap_or_else(|e| NeighborsResponse {
        title: title_for_err,
        total: 0,
        neighbors: vec![],
        error: Some(format!("Internal error: {}", e)),
    });
    Json(result).into_response()
}

fn do_neighbors(graph: &LoadedGraph, titles: &TitleIndex, title: &str, limit: usize) -> NeighborsResponse {
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

/// Axum web server — stretch goal.
///
/// Routes:
///   GET  /          → serves the embedded HTML UI
///   POST /search    → JSON {from: string, to: string} → {path, hops, ms, error?}
use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    build_cmd::TitleIndex,
    graph::LoadedGraph,
    search::shortest_path,
};

struct AppState {
    graph: LoadedGraph,
    titles: TitleIndex,
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

pub async fn serve(port: u16, graph: LoadedGraph, titles: TitleIndex) {
    let state = Arc::new(AppState { graph, titles });

    let app = Router::new()
        .route("/", get(index_handler))
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

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SearchRequest>,
) -> impl IntoResponse {
    // Normalise: Wikipedia titles use underscores
    let from_key = req.from.replace(' ', "_");
    let to_key = req.to.replace(' ', "_");

    let start_cid = match state.titles.title_to_cid.get(&from_key) {
        Some(&c) => c,
        None => {
            return (
                StatusCode::OK,
                Json(SearchResponse {
                    path: vec![],
                    hops: 0,
                    ms: 0,
                    error: Some(format!("Article not found: \"{}\"", req.from)),
                }),
            );
        }
    };
    let end_cid = match state.titles.title_to_cid.get(&to_key) {
        Some(&c) => c,
        None => {
            return (
                StatusCode::OK,
                Json(SearchResponse {
                    path: vec![],
                    hops: 0,
                    ms: 0,
                    error: Some(format!("Article not found: \"{}\"", req.to)),
                }),
            );
        }
    };

    match shortest_path(&state.graph.forward, &state.graph.backward, start_cid, end_cid) {
        Some(result) => {
            let path_titles: Vec<String> = result
                .path
                .iter()
                .map(|&cid| {
                    state
                        .titles
                        .titles
                        .get(cid as usize)
                        .cloned()
                        .unwrap_or_else(|| format!("#{}", cid))
                })
                .collect();
            (
                StatusCode::OK,
                Json(SearchResponse {
                    path: path_titles,
                    hops: result.hops,
                    ms: result.elapsed_ms,
                    error: None,
                }),
            )
        }
        None => (
            StatusCode::OK,
            Json(SearchResponse {
                path: vec![],
                hops: 0,
                ms: 0,
                error: Some("No path found between the two articles.".to_string()),
            }),
        ),
    }
}

async fn neighbors_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<NeighborsRequest>,
) -> impl IntoResponse {
    let key = req.title.replace(' ', "_");
    let title_for_response = req.title.clone();

    let cid = match state.titles.title_to_cid.get(&key) {
        Some(&c) => c,
        None => {
            return (
                StatusCode::OK,
                Json(NeighborsResponse {
                    title: title_for_response,
                    total: 0,
                    neighbors: vec![],
                    error: Some(format!("Article not found: \"{}\"", req.title)),
                }),
            );
        }
    };

    // Gather outbound neighbor titles, capped to `limit`.
    let neighbors_raw = state.graph.forward.neighbors(cid);
    let total = neighbors_raw.len();
    let limit = req.limit.max(1).min(500);
    let neighbors: Vec<String> = neighbors_raw
        .iter()
        .take(limit)
        .map(|&n| {
            state
                .titles
                .titles
                .get(n as usize)
                .cloned()
                .unwrap_or_else(|| format!("#{}", n))
        })
        .collect();

    (
        StatusCode::OK,
        Json(NeighborsResponse {
            title: req.title,
            total,
            neighbors,
            error: None,
        }),
    )
}

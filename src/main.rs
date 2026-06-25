mod build_cmd;
mod download;
mod fetch;
mod graph;
mod parse;
mod progress;
mod search;
mod setup;
mod stats;
mod web;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use setup::AppHandle;

// ── CLI definition ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "wikigame",
    about = "Wikipedia shortest-path finder — bidirectional BFS on the full English Wikipedia link graph"
)]
struct Cli {
    /// Directory for processed graph files.
    #[arg(long, global = true, default_value = "data/processed")]
    data: PathBuf,

    /// Directory for downloaded dump files.
    #[arg(long, global = true, default_value = "data/downloads")]
    downloads: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Download the three Wikipedia SQL dumps from Wikimedia (resumes interrupted downloads).
    Download,

    /// Fetch prebuilt graph `.bin` files from a mirror (Cloudflare R2) instead
    /// of downloading dumps and building locally. Much faster (minutes vs hours).
    Fetch {
        /// Mirror base URL. Defaults to the `WIKIGAME_MIRROR_URL` env var, then
        /// the built-in default.
        #[arg(long)]
        base_url: Option<String>,
    },

    /// Parse Wikipedia SQL dumps and build CSR binary files (one-time, ~1-3 h).
    Build {
        /// Skip downloading even if dump files are absent (fail instead).
        #[arg(long)]
        no_download: bool,

        /// Keep the downloaded .sql.gz files after parsing (they consume ~10 GB).
        #[arg(long)]
        keep_dumps: bool,
    },

    /// Find the shortest path between two Wikipedia articles.
    Search {
        /// Title of the starting article (spaces or underscores both work).
        from: Option<String>,

        /// Title of the destination article.
        to: Option<String>,

        /// Stay in a loop so the graph is loaded only once.
        #[arg(long, short = 'i')]
        interactive: bool,
    },

    /// Start the local web server (http://localhost:<port>).
    Serve {
        /// Port to listen on.
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
}

// ── Entry point ────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match cli.command {
        None => run_default(cli.data, cli.downloads),
        Some(Commands::Download) => {
            println!("=== wikigame download ===");
            let reporter = progress::ProgressReporter::standalone(64);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(download::download_all(&cli.downloads, &reporter));
        }
        Some(Commands::Fetch { base_url }) => {
            println!("=== wikigame fetch ===");
            let url = fetch::mirror_url(base_url.as_deref());
            println!("Mirror: {}", url);
            let reporter = progress::ProgressReporter::standalone(64);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(fetch::fetch_all(&cli.data, &url, &reporter));
            println!("Done. Graph ready in {:?}.", cli.data);
        }
        Some(Commands::Build {
            no_download,
            keep_dumps,
        }) => {
            println!("=== wikigame build ===");
            let reporter = progress::ProgressReporter::standalone(64);
            let can_resume =
                cli.data.join("edges.tmp").exists() && cli.data.join("title_index.bin").exists();
            if !can_resume && !no_download && !download::all_present(&cli.downloads) {
                println!("Dump files not found. Downloading now ...");
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(download::download_all(&cli.downloads, &reporter));
            } else if can_resume {
                println!("Resuming from edges.tmp + title_index.bin.");
            }
            build_cmd::run(&cli.downloads, &cli.data, !keep_dumps, &reporter);
        }
        Some(Commands::Search {
            from,
            to,
            interactive,
        }) => {
            println!("=== wikigame search ===");
            let (graph, titles) = setup::load_graph(&cli.data).unwrap_or_else(|| {
                panic!(
                    "Graph not found in {:?}. Run `wikigame` or `wikigame build` first.",
                    cli.data
                )
            });
            if interactive {
                run_interactive(&graph, &titles);
            } else {
                let from = from.unwrap_or_else(|| {
                    eprintln!("Error: <FROM> title is required in non-interactive mode.");
                    std::process::exit(1);
                });
                let to = to.unwrap_or_else(|| {
                    eprintln!("Error: <TO> title is required in non-interactive mode.");
                    std::process::exit(1);
                });
                do_search(&graph, &titles, &from, &to);
            }
        }
        Some(Commands::Serve { port }) => {
            println!("=== wikigame serve (port {}) ===", port);
            let handle = AppHandle::new(cli.downloads.clone(), cli.data.clone());
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    try_open_browser(port);
                    web::serve(port, handle).await;
                });
        }
    }
}

// ── Default flow ───────────────────────────────────────────────────────────────

fn run_default(data_dir: PathBuf, downloads_dir: PathBuf) {
    println!("=== wikigame ===");
    let port = 8080;
    let handle = AppHandle::new(downloads_dir, data_dir);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            try_open_browser(port);
            web::serve(port, handle).await;
        });
}

fn try_open_browser(port: u16) {
    let url = format!("http://localhost:{}", port);
    std::thread::spawn(move || {
        if let Err(e) = open::that(&url) {
            eprintln!("Could not open browser automatically: {}", e);
            eprintln!("Open {} in your browser.", url);
        }
    });
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn do_search(graph: &graph::LoadedGraph, titles: &build_cmd::TitleIndex, from: &str, to: &str) {
    let from_key = from.replace(' ', "_");
    let to_key = to.replace(' ', "_");

    let start_cid = match titles.title_to_cid.get(&from_key) {
        Some(&c) => c,
        None => {
            eprintln!("Article not found: \"{}\"", from);
            return;
        }
    };

    let end_cid = match titles.title_to_cid.get(&to_key) {
        Some(&c) => c,
        None => {
            eprintln!("Article not found: \"{}\"", to);
            return;
        }
    };

    print!("Searching '{}' -> '{}' ... ", from, to);

    match search::shortest_path(&graph.forward, &graph.backward, start_cid, end_cid) {
        None => println!("\nNo path found."),
        Some(result) => {
            println!("({} hops, {} ms)", result.hops, result.elapsed_ms);
            for (i, &cid) in result.path.iter().enumerate() {
                let title = titles
                    .titles
                    .get(cid as usize)
                    .map(String::as_str)
                    .unwrap_or("?");
                if i == 0 {
                    println!("  → {}", title);
                } else {
                    println!(
                        "  → {} (https://en.wikipedia.org/wiki/{})",
                        title,
                        title.replace(' ', "_")
                    );
                }
            }
        }
    }
}

fn run_interactive(graph: &graph::LoadedGraph, titles: &build_cmd::TitleIndex) {
    use std::io::{self, BufRead, Write};

    println!("Interactive mode. Type two article titles, one per prompt. Ctrl-C to quit.\n");

    let stdin = io::stdin();
    loop {
        print!("From: ");
        io::stdout().flush().ok();
        let mut from = String::new();
        if stdin.lock().read_line(&mut from).unwrap_or(0) == 0 {
            break;
        }
        let from = from.trim().to_string();
        if from.is_empty() {
            continue;
        }

        print!("To:   ");
        io::stdout().flush().ok();
        let mut to = String::new();
        if stdin.lock().read_line(&mut to).unwrap_or(0) == 0 {
            break;
        }
        let to = to.trim().to_string();
        if to.is_empty() {
            continue;
        }

        do_search(graph, titles, &from, &to);
        println!();
    }
}

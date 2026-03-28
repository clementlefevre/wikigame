mod build_cmd;
mod download;
mod graph;
mod parse;
mod search;
mod web;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use build_cmd::TitleIndex;

// ── CLI definition ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "wikigame",
    about = "Wikipedia shortest-path finder — bidirectional BFS on the full English Wikipedia link graph"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download the three Wikipedia SQL dumps from Wikimedia (resumes interrupted downloads).
    Download {
        /// Directory where the .sql.gz files will be saved.
        #[arg(long, default_value = "data/downloads")]
        output: PathBuf,
    },

    /// Parse Wikipedia SQL dumps and build CSR binary files (one-time, ~1-3 h).
    Build {
        /// Directory containing the three .sql.gz dumps.
        /// If the files are missing, they will be downloaded automatically.
        #[arg(long, default_value = "data/downloads")]
        downloads: PathBuf,

        /// Directory where processed binary files will be written.
        #[arg(long, default_value = "data/processed")]
        output: PathBuf,

        /// Skip downloading even if dump files are absent (fail instead).
        #[arg(long)]
        no_download: bool,
    },

    /// Find the shortest path between two Wikipedia articles.
    Search {
        /// Title of the starting article (spaces or underscores both work).
        from: Option<String>,

        /// Title of the destination article.
        to: Option<String>,

        /// Directory containing processed binary files.
        #[arg(long, default_value = "data/processed")]
        data: PathBuf,

        /// Stay in a loop so the graph is loaded only once.
        #[arg(long, short = 'i')]
        interactive: bool,
    },

    /// Start the local web server (http://localhost:<port>).
    Serve {
        /// Port to listen on.
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Directory containing processed binary files.
        #[arg(long, default_value = "data/processed")]
        data: PathBuf,
    },
}

// ── Entry point ────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Download { output } => {
            println!("=== wikigame download ===");
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(download::download_all(&output));
        }

        Commands::Build {
            downloads,
            output,
            no_download,
        } => {
            println!("=== wikigame build ===");
            if !no_download && !download::all_present(&downloads) {
                println!("Dump files not found in {:?}. Downloading now ...", downloads);
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(download::download_all(&downloads));
            }
            build_cmd::run(&downloads, &output);
        }

        Commands::Search {
            from,
            to,
            data,
            interactive,
        } => {
            println!("=== wikigame search ===");
            let titles = load_title_index(&data);
            let graph = graph::load(&data);

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

        Commands::Serve { port, data } => {
            println!("=== wikigame serve (port {}) ===", port);
            let titles = load_title_index(&data);
            let graph = graph::load(&data);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(web::serve(port, graph, titles));
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn load_title_index(data_dir: &PathBuf) -> TitleIndex {
    let path = data_dir.join("title_index.bin");
    let f = std::fs::File::open(&path)
        .unwrap_or_else(|_| panic!("Cannot open {:?}. Did you run `wikigame build` first?", path));
    bincode::deserialize_from(std::io::BufReader::new(f))
        .unwrap_or_else(|e| panic!("Failed to deserialize title_index.bin: {}", e))
}

fn do_search(
    graph: &graph::LoadedGraph,
    titles: &TitleIndex,
    from: &str,
    to: &str,
) {
    // Normalise: Wikipedia titles use underscores
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
                let title = titles.titles.get(cid as usize).map(String::as_str).unwrap_or("?");
                if i == 0 {
                    println!("  → {}", title);
                } else {
                    println!("  → {} (https://en.wikipedia.org/wiki/{})", title, title.replace(' ', "_"));
                }
            }
        }
    }
}

fn run_interactive(graph: &graph::LoadedGraph, titles: &TitleIndex) {
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


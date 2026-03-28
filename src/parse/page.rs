/// Parse `enwiki-latest-page.sql.gz`.
///
/// Schema (namespace 0, non-redirect rows only):
///   page_id       INT  UNSIGNED  col 0
///   page_namespace INT           col 1
///   page_title     VARBINARY(255)col 2
///   page_is_redirect TINYINT     col 3
///
/// Output:
///   - `wiki_id_to_cid`  : HashMap<u32 wiki_page_id → u32 compact_id>
///   - `titles`          : Vec<String>  (index == compact_id)
///   - `title_to_cid`    : HashMap<String → compact_id>
use std::path::Path;

use hashbrown::HashMap;
use indicatif::{ProgressBar, ProgressStyle};

use super::{rows, SqlValue};

pub struct PageIndex {
    pub wiki_id_to_cid: HashMap<u32, u32>,
    pub titles: Vec<String>,
    pub title_to_cid: HashMap<String, u32>,
}

pub fn parse(path: &Path) -> PageIndex {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    pb.set_message("Parsing page.sql.gz …");

    let mut wiki_id_to_cid: HashMap<u32, u32> = HashMap::new();
    let mut titles: Vec<String> = Vec::new();
    let mut title_to_cid: HashMap<String, u32> = HashMap::new();

    for row in rows(path) {
        // Need at least 4 columns
        if row.len() < 4 {
            continue;
        }

        // col 1: page_namespace — keep only namespace 0
        let ns = match row[1].as_i64() {
            Some(n) => n,
            None => continue,
        };
        if ns != 0 {
            continue;
        }

        // col 3: page_is_redirect — skip redirects
        let is_redirect = match row[3].as_i64() {
            Some(n) => n,
            None => continue,
        };
        if is_redirect != 0 {
            continue;
        }

        // col 0: page_id
        let wiki_id = match row[0].as_i64() {
            Some(n) if n > 0 => n as u32,
            _ => continue,
        };

        // col 2: page_title (underscores are part of the dump format, keep as-is)
        let title = match &row[2] {
            SqlValue::Str(s) => s.clone(),
            _ => continue,
        };

        let cid = titles.len() as u32;
        wiki_id_to_cid.insert(wiki_id, cid);
        title_to_cid.insert(title.clone(), cid);
        titles.push(title);

        if titles.len() % 500_000 == 0 {
            pb.set_message(format!("Parsing page.sql.gz … {} articles", titles.len()));
        }
    }

    pb.finish_with_message(format!(
        "page.sql.gz done — {} main-namespace articles",
        titles.len()
    ));

    PageIndex {
        wiki_id_to_cid,
        titles,
        title_to_cid,
    }
}

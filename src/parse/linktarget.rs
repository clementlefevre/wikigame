/// Parse `enwiki-latest-linktarget.sql.gz`.
///
/// Schema:
///   lt_id         BIGINT UNSIGNED   col 0
///   lt_namespace  INT               col 1
///   lt_title      VARBINARY(255)    col 2
///
/// We only keep rows where lt_namespace == 0 AND lt_title is a known article.
/// Returns a flat map: lt_id → compact_id.
use std::path::Path;

use hashbrown::HashMap;

use crate::progress::ProgressReporter;

pub fn parse(
    path: &Path,
    title_to_cid: &HashMap<String, u32>,
    reporter: &ProgressReporter,
) -> HashMap<u64, u32> {
    reporter.phase("Parsing", "linktarget.sql.gz …");

    let mut lt_to_cid: HashMap<u64, u32> = HashMap::new();
    let mut count = 0u64;

    for row in super::rows(path) {
        if row.len() < 3 {
            continue;
        }

        // col 1: lt_namespace
        let ns = match row[1].as_i64() {
            Some(n) => n,
            None => continue,
        };
        if ns != 0 {
            continue;
        }

        // col 2: lt_title
        let title = match &row[2] {
            super::SqlValue::Str(s) => s.as_str(),
            _ => continue,
        };

        if let Some(&cid) = title_to_cid.get(title) {
            // col 0: lt_id
            let lt_id = match row[0].as_i64() {
                Some(n) if n > 0 => n as u64,
                _ => continue,
            };
            lt_to_cid.insert(lt_id, cid);
        }

        count += 1;
        if count % 1_000_000 == 0 {
            reporter.progress(
                "Parsing",
                format!(
                    "linktarget.sql.gz — {} rows ({} mapped)",
                    count,
                    lt_to_cid.len()
                ),
                count,
                0,
            );
        }
    }

    reporter.log(
        "Parsing",
        format!(
            "linktarget.sql.gz done — {} link targets mapped",
            lt_to_cid.len()
        ),
    );
    lt_to_cid
}

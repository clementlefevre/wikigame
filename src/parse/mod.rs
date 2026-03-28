/// Streaming SQL INSERT tokenizer for Wikipedia dumps.
///
/// Wikipedia SQL dumps consist of a series of multi-valued INSERT statements:
///   INSERT INTO `table` VALUES (v1,v2,...),(v1,v2,...), ...;
///
/// This module exposes a line iterator that reads a `.sql.gz` file and yields
/// rows as `Vec<SqlValue>`.  The caller never has to touch the gzip or string
/// escaping machinery.
pub mod page;
pub mod linktarget;
pub mod pagelinks;

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use flate2::read::GzDecoder;

// ── Value type ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum SqlValue {
    Int(i64),
    Str(String),
    Null,
}

impl SqlValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SqlValue::Int(n) => Some(*n),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqlValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

// ── Public iterator ────────────────────────────────────────────────────────────

/// Returns an iterator that yields every SQL row (as `Vec<SqlValue>`) found
/// in an `INSERT INTO … VALUES …` statement inside a gzip-compressed SQL file.
pub fn rows(path: &Path) -> impl Iterator<Item = Vec<SqlValue>> {
    let file = File::open(path).unwrap_or_else(|e| panic!("Cannot open {:?}: {}", path, e));
    let gz = GzDecoder::new(file);
    let reader = BufReader::with_capacity(4 * 1024 * 1024, gz);

    reader
        .lines()
        .filter_map(|line_result| {
            let line = line_result.ok()?;
            // Only process INSERT INTO VALUE lines
            if !line.starts_with("INSERT INTO") {
                return None;
            }
            let values_pos = line.find(" VALUES ")?;
            // The payload starts after " VALUES "
            Some(line[values_pos + 8..].to_string())
        })
        .flat_map(|values_str| {
            // Each VALUES line can contain thousands of tuples: (…),(…),…;
            parse_value_list(&values_str)
        })
}

// ── Parser internals ──────────────────────────────────────────────────────────

/// Parse the VALUES payload of an INSERT statement into a `Vec<Vec<SqlValue>>`.
/// The input looks like: `(1,'hello',NULL),(2,'world',42);`
fn parse_value_list(s: &str) -> Vec<Vec<SqlValue>> {
    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    let bytes = s.as_bytes();
    let mut pos = 0;
    let len = bytes.len();

    while pos < len {
        // skip to next '('
        while pos < len && bytes[pos] != b'(' {
            pos += 1;
        }
        if pos >= len {
            break;
        }
        pos += 1; // consume '('

        let mut row: Vec<SqlValue> = Vec::new();

        loop {
            // skip whitespace
            while pos < len && (bytes[pos] == b' ' || bytes[pos] == b'\t') {
                pos += 1;
            }
            if pos >= len {
                break;
            }

            let val = match bytes[pos] {
                b'\'' => {
                    // quoted string
                    pos += 1; // consume opening quote
                    let s = read_quoted_string(bytes, &mut pos);
                    SqlValue::Str(s)
                }
                b'N' if bytes.get(pos..pos + 4) == Some(b"NULL") => {
                    pos += 4;
                    SqlValue::Null
                }
                b')' => break,
                _ => {
                    // integer (or float — cast to i64)
                    let start = pos;
                    // allow leading minus
                    if pos < len && bytes[pos] == b'-' {
                        pos += 1;
                    }
                    while pos < len && bytes[pos].is_ascii_digit() {
                        pos += 1;
                    }
                    // skip decimal part if present (we don't need floats)
                    if pos < len && bytes[pos] == b'.' {
                        pos += 1;
                        while pos < len && bytes[pos].is_ascii_digit() {
                            pos += 1;
                        }
                    }
                    let token = std::str::from_utf8(&bytes[start..pos]).unwrap_or("0");
                    let n: i64 = token
                        .parse::<f64>()
                        .map(|f| f as i64)
                        .unwrap_or(0);
                    SqlValue::Int(n)
                }
            };

            row.push(val);

            // expect ',' or ')' after value
            while pos < len && bytes[pos] == b' ' {
                pos += 1;
            }
            match bytes.get(pos) {
                Some(b',') => {
                    pos += 1;
                }
                Some(b')') | None => break,
                _ => {
                    // Malformed; skip to closing paren
                    while pos < len && bytes[pos] != b')' {
                        pos += 1;
                    }
                    break;
                }
            }
        }

        // consume ')'
        while pos < len && bytes[pos] != b')' {
            pos += 1;
        }
        if pos < len {
            pos += 1;
        }

        if !row.is_empty() {
            rows.push(row);
        }
    }

    rows
}

/// Read a MySQL-escaped quoted string starting at `bytes[*pos]` (the byte
/// *after* the opening quote).  Updates `*pos` to point past the closing quote.
fn read_quoted_string(bytes: &[u8], pos: &mut usize) -> String {
    let mut out = Vec::with_capacity(64);
    let len = bytes.len();

    while *pos < len {
        match bytes[*pos] {
            b'\\' => {
                *pos += 1;
                if *pos >= len {
                    break;
                }
                let escaped = match bytes[*pos] {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'0' => b'\0',
                    b'Z' => 0x1A, // Ctrl-Z
                    other => other,
                };
                out.push(escaped);
                *pos += 1;
            }
            b'\'' => {
                *pos += 1;
                // MySQL doubles quotes to escape them: ''
                if *pos < len && bytes[*pos] == b'\'' {
                    out.push(b'\'');
                    *pos += 1;
                } else {
                    // End of string
                    break;
                }
            }
            b => {
                out.push(b);
                *pos += 1;
            }
        }
    }

    // Produce a UTF-8 string; replace invalid bytes rather than panicking.
    String::from_utf8_lossy(&out).into_owned()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_tuple() {
        let rows = parse_value_list("(1,'hello',NULL),(2,'wo\\'rld',42);");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_i64(), Some(1));
        assert_eq!(rows[0][1].as_str(), Some("hello"));
        assert!(matches!(rows[0][2], SqlValue::Null));
        assert_eq!(rows[1][1].as_str(), Some("wo'rld"));
        assert_eq!(rows[1][2].as_i64(), Some(42));
    }

    #[test]
    fn parse_negative_int() {
        let rows = parse_value_list("(-5,'x',-10);");
        assert_eq!(rows[0][0].as_i64(), Some(-5));
        assert_eq!(rows[0][2].as_i64(), Some(-10));
    }
}

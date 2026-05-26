use crate::DbConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use regex::Regex;

use rusqlite::Row;
use rusqlite::types::Value as SqlValue;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub timestamp: DateTime<Utc>,
    pub video_id: String,
    pub video_offset_time_seconds: i64,
    pub message: String,
    pub author: String,
    pub title: String,
    pub video_offset_time_msec: Option<i64>,
}

const FETCH_QUERY: &str = r#"
    SELECT
        lc.timestamp,
        lc.video_id,
        CAST(ROUND(
            (julianday(lc.timestamp) - julianday(vm.release_timestamp)) * 86400
        ) AS INTEGER),
        lc.message,
        lc.author,
        vm.title,
        lc.video_offset_time_msec
    FROM live_chat lc
    JOIN video_metadata vm ON lc.video_id = vm.video_id
"#;

fn map_row(row: &Row<'_>) -> rusqlite::Result<SearchRow> {
    Ok(SearchRow {
        timestamp: row.get(0)?,
        video_id: row.get(1)?,
        video_offset_time_seconds: row.get::<_, i64>(2).unwrap_or(0),
        message: row.get(3)?,
        author: row.get(4)?,
        title: row.get(5)?,
        video_offset_time_msec: row.get(6).ok(),
    })
}

/// Fetch total row count from live_chat table
fn fetch_total(conn: &rusqlite::Connection) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM live_chat",
        [],
        |row| row.get(0),
    )
    .with_context(|| "Failed to count live_chat rows")
}

/// Fetch the latest timestamp from live_chat table
fn fetch_latest(conn: &rusqlite::Connection) -> Result<Option<DateTime<Utc>>> {
    conn.query_row(
        "SELECT MAX(timestamp) FROM live_chat",
        [],
        |row| row.get(0),
    )
    .with_context(|| "Failed to get latest timestamp")
}

pub fn search_messages(
    db_config: &DbConfig,
    regex_patterns: &[String],
    window_size: i64,
    min_matches: usize,
) -> Result<(Vec<SearchRow>, Option<DateTime<Utc>>, i64)> {
    use rusqlite::Connection;
    let conn_path = db_config.connect_path()?;
    let conn = Connection::open(conn_path)
        .context("Failed to open SQLite database")?;

    // Compile regex patterns for Rust-level filtering (fallback)
    let match_patterns: Vec<Regex> = regex_patterns
        .iter()
        .map(|p| Regex::new(&format!("(?i){}", p)))
        .collect::<Result<_, _>>()
        .context("Failed to compile regex patterns")?;

    // Build SQLite LIKE params with % wildcards
    let like_params: Vec<SqlValue> = regex_patterns
        .iter()
        .map(|p| format!("%{}%", p).into())
        .collect();

    // Try the filtered query first (LIKE can speed up the common case where
    // the pattern contains a simple substring), fall back to full scan if needed.
    let data: Vec<SearchRow> = {
        // Build WHERE clause with SQLite LIKE
        let like_placeholders: String = (0..like_params.len())
            .map(|i| format!("lc.message LIKE ?{}", i + 1))
            .collect::<Vec<_>>()
            .join(" OR ");
        let query = format!(
            "{} WHERE {} ORDER BY lc.video_id, lc.timestamp",
            FETCH_QUERY, like_placeholders
        );

        let mut data: Vec<SearchRow> = Vec::new();

        // Try filtered query
        if let Ok(mut stmt) = conn.prepare(&query) {
            if let Ok(rows_iter) = stmt.query_map(
                rusqlite::params_from_iter(like_params.iter().cloned()),
                map_row,
            ) {
                data = rows_iter
                    .flatten()
                    .filter(|r| match_patterns.iter().any(|re| re.is_match(&r.message)))
                    .collect();
            }
        }

        // If the LIKE filter returned no rows (e.g. the pattern uses regex syntax
        // that LIKE doesn't understand), fall back to full scan + in-memory regex
        if data.is_empty() {
            if let Ok(mut stmt) = conn.prepare(FETCH_QUERY) {
                if let Ok(rows_iter) = stmt.query_map([], map_row) {
                    data = rows_iter
                        .flatten()
                        .filter(|r| match_patterns.iter().any(|re| re.is_match(&r.message)))
                        .collect();
                }
            }
        }

        data
    };

    // Compute total and latest
    let total = fetch_total(&conn)?;
    let latest = fetch_latest(&conn)?;

    // Group by video_id, sort each group by timestamp, then apply windowing
    let mut groups: HashMap<String, Vec<SearchRow>> = HashMap::new();
    for row in data {
        groups.entry(row.video_id.clone()).or_default().push(row);
    }

    let mut results: Vec<SearchRow> = Vec::new();
    for (video_id, mut group) in groups {
        group.sort_by_key(|r| r.timestamp);
        for i in 0..group.len() {
            let row = &group[i];
            let window_end = row.timestamp + Duration::seconds(window_size);
            let count = group
                .iter()
                .filter(|r| r.timestamp >= row.timestamp && r.timestamp < window_end)
                .count();
            if count >= min_matches {
                let already_added = results.last().map_or(false, |last| {
                    last.video_id == video_id
                        && (row.timestamp - last.timestamp).num_seconds() < window_size
                });
                if !already_added {
                    results.push(group[i].clone());
                }
            }
        }
    }

    results.sort_by_key(|r| r.timestamp);

    Ok((results, latest, total))
}

pub fn print_search_results(
    db_config: &DbConfig,
    regex_patterns: &[String],
    window_size: i64,
    min_matches: usize,
    timestamp_offset: i64,
    output_file: Option<&str>,
    debug: bool,
) -> Result<()> {
    let (results, latest, total_lines) =
        search_messages(db_config, regex_patterns, window_size, min_matches)?;

    let mut headers = vec!["Date", "Title", "Timestamp"];
    if debug {
        headers.extend(["Author", "Message"]);
    }

    let header_line = format!("| {} |", headers.join(" | "));
    let spacer_line = format!("|{}|", vec!["------"; headers.len()].join("|"));
    let mut lines = vec![header_line, spacer_line];

    // Group hits by video_id, preserving the order of first occurrence
    let mut groups: Vec<(String, Vec<&SearchRow>)> = Vec::new();
    for r in &results {
        match groups.iter_mut().find(|(vid, _)| vid == &r.video_id) {
            Some((_, rows)) => rows.push(r),
            None => groups.push((r.video_id.clone(), vec![r])),
        }
    }

    for (_, rows) in &groups {
        let first = rows[0];
        let video_link = format!("https://www.youtube.com/watch?v={}", first.video_id);

        let timestamps: Vec<String> = rows
            .iter()
            .map(|r| {
                let offset_secs = if r.video_offset_time_msec.unwrap_or(0) > 0 {
                    r.video_offset_time_msec.unwrap() / 1000
                } else {
                    r.video_offset_time_seconds
                };
                let adjusted = (offset_secs + timestamp_offset).max(0);
                let h = adjusted / 3600;
                let m = (adjusted % 3600) / 60;
                let s = adjusted % 60;
                format!("[{:02}:{:02}:{:02}]({}&t={}s)", h, m, s, video_link, adjusted)
            })
            .collect();

        let mut row = vec![
            first.timestamp.format("%Y-%m-%d").to_string(),
            format!("[{}]({})", first.title, video_link),
            timestamps.join(", "),
        ];
        if debug {
            row.push(first.author.clone());
            row.push(first.message.clone());
        }
        lines.push(format!("| {} |", row.join(" | ")));
    }

    let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    lines.push(String::new());
    lines.push("| Parameter       | Value |".to_string());
    lines.push("|-----------------|-------|".to_string());
    let display_patterns: Vec<String> = regex_patterns
        .iter()
        .map(|p| p.replace('|', r"\|"))
        .collect();
    lines.push(format!(
        "| Search Patterns | `{}` |",
        display_patterns.join(", ")
    ));
    lines.push(format!("| Window Size     | {} seconds |", window_size));
    lines.push(format!("| Minimum Matches | {} |", min_matches));
    lines.push(format!("| Results Found   | {} |", results.len()));
    lines.push(format!("| Lines Searched  | {} |", total_lines));
    lines.push(format!("| Generated At    | {} |", now));
    if let Some(ts) = latest {
        lines.push(format!(
            "| Latest Live Chat | {} |",
            ts.format("%Y-%m-%d %H:%M:%S UTC")
        ));
    }

    let markdown = lines.join("\n");
    if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        termimad::print_text(&markdown);
    } else {
        println!("{}", markdown);
    }

    if let Some(path) = output_file {
        std::fs::write(path, &markdown)?;
    }

    Ok(())
}

pub fn db_check(db_config: &DbConfig) -> Result<()> {
    let conn_path = db_config.connect_path()?;
    println!("Opening SQLite database at {}...", conn_path.display());

    use rusqlite::Connection;
    let conn = Connection::open(&conn_path)
        .context("Failed to open database")?;

    let version: String = conn
        .query_row("SELECT sqlite_version()", [], |row| row.get(0))
        .context("Failed to get SQLite version")?;
    println!("OK — SQLite {}", version);

    let tables = [
        ("live_chat", "SELECT COUNT(*) FROM live_chat"),
        ("video_metadata", "SELECT COUNT(*) FROM video_metadata"),
    ];
    for (name, query) in &tables {
        match conn.query_row(*query, [], |row| row.get::<_, i64>(0)) {
            Ok(count) => {
                println!("  {}: {} rows", name, count);
            }
            Err(e) => println!("  {}: error — {}", name, e),
        }
    }

    Ok(())
}

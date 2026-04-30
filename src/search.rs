use crate::DbConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use postgres::{Client, NoTls};
use regex::Regex;
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
        CEIL(EXTRACT(EPOCH FROM AGE(lc.timestamp, vm.release_timestamp)))::bigint,
        lc.message,
        lc.author,
        vm.title,
        lc.video_offset_time_msec
    FROM live_chat lc
    JOIN video_metadata vm ON lc.video_id = vm.video_id
"#;

fn map_row(row: &postgres::Row) -> SearchRow {
    SearchRow {
        timestamp: row.get(0),
        video_id: row.get(1),
        video_offset_time_seconds: row.get::<_, i64>(2),
        message: row.get(3),
        author: row.get(4),
        title: row.get(5),
        video_offset_time_msec: row.get(6),
    }
}

pub fn search_messages(
    db_config: &DbConfig,
    regex_patterns: &[String],
    window_size: i64,
    min_matches: usize,
) -> Result<(Vec<SearchRow>, Option<DateTime<Utc>>, i64)> {
    let mut client = Client::connect(&db_config.conn_string(), NoTls)?;

    // Build WHERE clause with DB-level regex filtering (case-insensitive ~*)
    // Patterns come from CLI args (not external input), single-quote escaping is sufficient
    let conditions: Vec<String> = regex_patterns
        .iter()
        .map(|p| format!("lc.message ~* '{}'", p.replace('\'', "''")))
        .collect();
    let where_clause = format!("WHERE {}", conditions.join(" OR "));

    let query = format!(
        "{} {} ORDER BY lc.video_id, lc.timestamp",
        FETCH_QUERY, where_clause
    );

    let data: Vec<SearchRow> = match client.query(&query, &[]) {
        Ok(rows) => rows.iter().map(map_row).collect(),
        Err(e) => {
            eprintln!("Database-level regex filtering failed: {}", e);
            eprintln!("Falling back to full scan with Rust-level filtering...");
            let fallback = format!(
                "{} ORDER BY lc.video_id, lc.timestamp",
                FETCH_QUERY
            );
            let rows = client.query(&fallback, &[])?;
            let compiled: Vec<Regex> = regex_patterns
                .iter()
                .map(|p| Regex::new(&format!("(?i){}", p)))
                .collect::<Result<_, _>>()?;
            rows.iter()
                .map(map_row)
                .filter(|r| compiled.iter().any(|re| re.is_match(&r.message)))
                .collect()
        }
    };

    let total: i64 = client
        .query_one("SELECT COUNT(*) FROM live_chat", &[])?
        .get(0);

    let latest: Option<DateTime<Utc>> = client
        .query_one("SELECT MAX(timestamp) FROM live_chat", &[])?
        .get(0);

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
                let add = results.last().map_or(true, |last| {
                    last.video_id != video_id
                        || (row.timestamp - last.timestamp).num_seconds() >= window_size
                });
                if add {
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
    lines.push(format!(
        "| Search Patterns | `{}` |",
        regex_patterns.join(", ")
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
    println!(
        "Connecting to PostgreSQL at {}:{}/{} as {}...",
        db_config.host, db_config.port, db_config.dbname, db_config.user
    );

    let mut client = Client::connect(&db_config.conn_string(), NoTls)
        .context("Failed to connect to database")?;

    let version: String = client.query_one("SELECT version()", &[])?.get(0);
    println!("OK — {}", version);

    let tables = [
        ("live_chat", "SELECT COUNT(*) FROM live_chat"),
        ("video_metadata", "SELECT COUNT(*) FROM video_metadata"),
    ];
    for (name, query) in &tables {
        match client.query_one(*query, &[]) {
            Ok(row) => {
                let count: i64 = row.get(0);
                println!("  {}: {} rows", name, count);
            }
            Err(e) => println!("  {}: error — {}", name, e),
        }
    }

    Ok(())
}

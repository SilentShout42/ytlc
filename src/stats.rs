use crate::DbConfig;
use anyhow::{Context, Result};
use rusqlite::OptionalExtension;
use std::collections::HashMap;

/// Parse a human-readable duration string like "2m", "1h", "30s" into seconds.
pub fn parse_duration(s: &str) -> Result<i64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    let mut total = 0i64;
    let mut current_num = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else {
            if current_num.is_empty() {
                anyhow::bail!("No number before unit '{}' in '{}'", ch, s);
            }
            let num = current_num.parse::<i64>().with_context(|| {
                format!("Failed to parse number '{}' from duration '{}'", current_num, s)
            })?;
            total += match ch {
                's' => num,
                'm' => num * 60,
                'h' => num * 3600,
                'd' => num * 86400,
                _ => anyhow::bail!("Unknown unit '{}' in duration '{}'", ch, s),
            };
            current_num.clear();
        }
    }

    if !current_num.is_empty() {
        anyhow::bail!("Trailing number '{}' without unit in '{}'", current_num, s);
    }

    if total == 0 {
        anyhow::bail!("Duration must be greater than 0");
    }

    Ok(total)
}

/// Ranking strategy for selecting top chat moments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RankStrategy {
    ZScoreUnique,
    UniqueAuthors,
    MessageRate,
    RollingPeak,
}

impl std::str::FromStr for RankStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "z-score-unique" | "zscore" => Ok(RankStrategy::ZScoreUnique),
            "unique-authors" | "unique" => Ok(RankStrategy::UniqueAuthors),
            "message-rate" | "rate" => Ok(RankStrategy::MessageRate),
            "rolling-peak" | "rolling" => Ok(RankStrategy::RollingPeak),
            other => Err(format!(
                "Unknown rank strategy '{}'. Valid options: z-score-unique, unique-authors, message-rate, rolling-peak",
                other
            )),
        }
    }
}

impl std::fmt::Display for RankStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RankStrategy::ZScoreUnique => write!(f, "z-score-unique"),
            RankStrategy::UniqueAuthors => write!(f, "unique-authors"),
            RankStrategy::MessageRate => write!(f, "message-rate"),
            RankStrategy::RollingPeak => write!(f, "rolling-peak"),
        }
    }
}

/// A single chunk with its computed statistics.
#[derive(Debug, Clone)]
pub struct ChunkStats {
    pub start_offset_sec: i64,
    pub end_offset_sec: i64,
    pub unique_authors: usize,
    pub total_messages: usize,
}

/// A top moment identified by a ranking strategy.
#[derive(Debug, Clone)]
pub struct TopMoment {
    pub chunk: ChunkStats,
    pub z_score: Option<f64>,
    pub message_rate: f64,
    /// For rolling-peak: the offset (in seconds) of the peak this moment precedes, or None.
    pub peak_offset_sec: Option<i64>,
    /// For rolling-peak: the unique count at the peak, or None.
    pub peak_unique: Option<usize>,
}

/// Format seconds into a human-readable time string.
/// If `max_total_secs` is provided, pads with leading zeros to match that width.
/// Otherwise falls back to `MM:SS` or `HH:MM:SS` depending on duration.
pub fn format_time(total_secs: i64) -> String {
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}

/// Format seconds into a fixed-width time string based on max duration.
pub fn format_time_fixed(total_secs: i64, max_total_secs: i64) -> String {
    let max_h = max_total_secs / 3600;

    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    // Pad to match the width of the max duration
    if max_h > 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}

/// Compute mean and standard deviation of unique author counts across chunks.
fn compute_mean_std(chunks: &[ChunkStats]) -> (f64, f64) {
    let n = chunks.len() as f64;
    if n == 0.0 {
        return (0.0, 0.0);
    }
    let sum: f64 = chunks.iter().map(|c| c.unique_authors as f64).sum();
    let mean = sum / n;
    let variance: f64 = chunks
        .iter()
        .map(|c| {
            let diff = c.unique_authors as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / n;
    let stddev = variance.sqrt();
    (mean, stddev)
}

/// Compute trailing rolling average over unique author counts.
/// Only looks backward (tail of the stream) to avoid future data leakage.
fn rolling_average(values: &[usize], window: usize) -> Vec<f64> {
    let n = values.len();
    if n == 0 || window == 0 {
        return vec![0.0; n];
    }
    let w = window.min(n);
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        let start = i.saturating_sub(w - 1);
        let end = i + 1;
        let count = (end - start) as f64;
        let sum: usize = values[start..end].iter().sum();
        result.push(sum as f64 / count);
    }
    result
}

/// Fetch all chat data for a video and compute chunk stats in a single pass.
/// Uses one SQL query ordered by offset, then buckets rows in Rust.
pub fn load_chunk_stats(
    conn: &rusqlite::Connection,
    video_id: &str,
    chunk_duration: i64,
    members_only: bool,
) -> Result<Vec<ChunkStats>> {
    use std::collections::HashSet;

    // Build WHERE clause fragment for optional member filter
    let member_filter = if members_only { "AND is_member = 1 " } else { "" };

    // Get time range
    let (max_offset, _total_messages): (i64, i64) = conn
        .query_row(
            format!(
                r#"
            SELECT
                COALESCE(MAX(video_offset_time_msec), 0),
                COUNT(*)
            FROM live_chat
            WHERE video_id = ?
              AND video_offset_time_msec IS NOT NULL
            {}"#,
                member_filter
            )
            .as_str(),
            [video_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .with_context(|| format!("No data for video ID: {}", video_id))
        .map(|opt| opt.unwrap_or((0, 0)))?;

    let total_secs = max_offset / 1000;
    let num_chunks: usize = ((total_secs / chunk_duration).max(1)) as usize;

    // Build chunks (zero-filled)
    let mut chunks: Vec<ChunkStats> = (0..num_chunks)
        .map(|i| ChunkStats {
            start_offset_sec: i as i64 * chunk_duration,
            end_offset_sec: (i as i64 + 1) * chunk_duration,
            unique_authors: 0,
            total_messages: 0,
        })
        .collect();

    // Single query: fetch all (author_channel_id, offset_msec) ordered by offset
    let query = format!(
        r#"
        SELECT author_channel_id,
               video_offset_time_msec
        FROM live_chat
        WHERE video_id = ?
          AND video_offset_time_msec IS NOT NULL
          {}ORDER BY video_offset_time_msec ASC
        "#,
        member_filter
    );

    let mut stmt = conn.prepare(&query)?;
    let chunk_msec = chunk_duration * 1000;

    // First pass: count total messages per chunk
    let mut rows = stmt.query(rusqlite::params![video_id])?;
    let mut msg_counts: Vec<usize> = vec![0usize; num_chunks];

    // We need to iterate twice: once for total messages, once for unique authors.
    // Collect all rows into memory for the second pass.
    // For large streams this is acceptable: each row is ~author_id (string) + offset (i64).
    // author_channel_id is typically ~22 chars, so each row is ~40 bytes.
    // A 100k-message stream is ~4MB — trivial.
    let mut all_rows: Vec<(String, i64)> = Vec::new();

    while let Some(row) = rows.next()? {
        let author_id: String = row.get(0)?;
        let offset_msec: i64 = row.get(1)?;

        let chunk_idx = (offset_msec / chunk_msec) as usize;
        if chunk_idx < num_chunks {
            msg_counts[chunk_idx] += 1;
        }
        all_rows.push((author_id, offset_msec));
    }

    // Second pass: bucket unique authors per chunk using HashMaps
    let mut chunk_authors: HashMap<usize, HashSet<String>> = HashMap::new();
    for (author_id, offset_msec) in &all_rows {
        let chunk_idx = (*offset_msec / chunk_msec) as usize;
        if chunk_idx < num_chunks {
            chunk_authors.entry(chunk_idx).or_insert_with(HashSet::new).insert(author_id.clone());
        }
    }

    // Assemble final chunks
    for (i, chunk) in chunks.iter_mut().enumerate() {
        chunk.total_messages = msg_counts[i];
        chunk.unique_authors = chunk_authors.get(&i).map_or(0, |s| s.len());
    }

    Ok(chunks)
}

/// Pick top N moments using the z-score-of-unique-authors strategy.
fn pick_z_score_top(chunks: &[ChunkStats], n: usize) -> Vec<TopMoment> {
    let (mean, stddev) = compute_mean_std(chunks);

    // If there's essentially no variance, fall back to raw unique-authors ranking
    let has_variance = stddev >= 1.0;

    let mut moments: Vec<TopMoment> = chunks
        .iter()
        .map(|chunk| {
            let duration = chunk.end_offset_sec - chunk.start_offset_sec;
            let duration_f = (duration as f64).max(1.0);
            TopMoment {
                chunk: chunk.clone(),
                z_score: if has_variance {
                    Some((chunk.unique_authors as f64 - mean) / stddev)
                } else {
                    None
                },
                message_rate: chunk.total_messages as f64 / duration_f,
                peak_offset_sec: None,
                peak_unique: None,
            }
        })
        .collect();

    moments.sort_by(|a, b| {
        let z_a = a.z_score.unwrap_or(f64::MIN);
        let z_b = b.z_score.unwrap_or(f64::MIN);
        z_b.partial_cmp(&z_a).unwrap_or(std::cmp::Ordering::Equal)
    });

    moments.into_iter().take(n).collect()
}

/// Pick the moment that precedes the rolling-averaged peak.
fn pick_rolling_peak_top(
    chunks: &[ChunkStats],
    _n: usize,
    rolling_window: usize,
    lookback_sec: i64,
) -> Vec<TopMoment> {
    let values: Vec<usize> = chunks.iter().map(|c| c.unique_authors).collect();
    let smoothed = rolling_average(&values, rolling_window);

    // Find the peak index of the smoothed series
    let peak_idx = smoothed
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0);

    let peak_offset_sec = chunks[peak_idx].start_offset_sec;
    let peak_unique = chunks[peak_idx].unique_authors;

    // Walk backward from peak to find the chunk closest to lookback_sec before it
    let target_offset = peak_offset_sec.saturating_sub(lookback_sec);
    let best_idx = smoothed
        .iter()
        .enumerate()
        .filter(|(i, _)| chunks[*i].start_offset_sec <= peak_offset_sec)
        .min_by_key(|(i, _)| {
            let dist = (chunks[*i].start_offset_sec as i64 - target_offset as i64).unsigned_abs();
            dist
        })
        .map(|(i, _)| i)
        .unwrap_or(peak_idx); // fallback: peak itself

    let chosen_chunk = &chunks[best_idx];
    let duration = chosen_chunk.end_offset_sec - chosen_chunk.start_offset_sec;
    let duration_f = (duration as f64).max(1.0);

    vec![TopMoment {
        chunk: chosen_chunk.clone(),
        z_score: None,
        message_rate: chosen_chunk.total_messages as f64 / duration_f,
        peak_offset_sec: Some(peak_offset_sec),
        peak_unique: Some(peak_unique),
    }]
}

/// Pick top N moments by raw unique author count.
fn pick_unique_authors_top(chunks: &[ChunkStats], n: usize) -> Vec<TopMoment> {
    let (mean, stddev) = compute_mean_std(chunks);
    let has_variance = stddev >= 1.0;

    let mut moments: Vec<TopMoment> = chunks
        .iter()
        .map(|chunk| {
            let duration = chunk.end_offset_sec - chunk.start_offset_sec;
            let duration_f = (duration as f64).max(1.0);
            TopMoment {
                chunk: chunk.clone(),
                z_score: if has_variance {
                    Some((chunk.unique_authors as f64 - mean) / stddev)
                } else {
                    None
                },
                message_rate: chunk.total_messages as f64 / duration_f,
                peak_offset_sec: None,
                peak_unique: None,
            }
        })
        .collect();

    moments.sort_by(|a, b| b.chunk.unique_authors.cmp(&a.chunk.unique_authors));
    moments.into_iter().take(n).collect()
}

/// Pick top N moments by message rate (messages per second).
fn pick_message_rate_top(chunks: &[ChunkStats], n: usize) -> Vec<TopMoment> {
    let (mean, stddev) = compute_mean_std(chunks);
    let has_variance = stddev >= 1.0;

    let mut moments: Vec<TopMoment> = chunks
        .iter()
        .map(|chunk| {
            let duration = chunk.end_offset_sec - chunk.start_offset_sec;
            let duration_f = (duration as f64).max(1.0);
            TopMoment {
                chunk: chunk.clone(),
                z_score: if has_variance {
                    Some((chunk.unique_authors as f64 - mean) / stddev)
                } else {
                    None
                },
                message_rate: chunk.total_messages as f64 / duration_f,
                peak_offset_sec: None,
                peak_unique: None,
            }
        })
        .collect();

    moments.sort_by(|a, b| b.message_rate.partial_cmp(&a.message_rate).unwrap_or(std::cmp::Ordering::Equal));
    moments.into_iter().take(n).collect()
}

/// Print the top moments table to stdout.
fn print_moments_table(video_id: &str, moments: &[TopMoment], strategy: &RankStrategy, max_total_secs: i64) {
    match strategy {
        RankStrategy::RollingPeak => {
            println!("| Rank | Time | Duration | Unique Authors | Messages | Peak At | Peak Uniques | Lookback |");
            println!("|------|------|----------|----------------|----------|---------|--------------|----------|");
            for (i, m) in moments.iter().enumerate() {
                let rank = i + 1;
                let time_label = format_time_fixed(m.chunk.start_offset_sec, max_total_secs);
                let youtube_url = format!("https://www.youtube.com/watch?v={}&t={}s", video_id, m.chunk.start_offset_sec);
                let link = format!("[{}]({})", time_label, youtube_url);
                let peak_at = m.peak_offset_sec.map(|p| format_time_fixed(p, max_total_secs)).unwrap_or_default();
                let peak_uniq = m.peak_unique.map(|u| u.to_string()).unwrap_or_default();
                let lookback = m.peak_offset_sec.map(|p| {
                    let diff = p - m.chunk.start_offset_sec;
                    format_time_fixed(diff.abs(), max_total_secs)
                }).unwrap_or_default();
                println!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} |",
                    rank, link, format_time_fixed(m.chunk.end_offset_sec - m.chunk.start_offset_sec, max_total_secs),
                    m.chunk.unique_authors, m.chunk.total_messages,
                    peak_at, peak_uniq, lookback,
                );
            }
        }
        RankStrategy::ZScoreUnique
        | RankStrategy::UniqueAuthors => {
            println!("| Rank | Time Range | Duration | Unique Authors | Messages | Z-Score |");
            println!("|------|------------|----------|----------------|----------|---------|");
            for (i, m) in moments.iter().enumerate() {
                let rank = i + 1;
                let start_label = format_time_fixed(m.chunk.start_offset_sec, max_total_secs);
                let end_label = format_time_fixed(m.chunk.end_offset_sec, max_total_secs);
                let youtube_url = format!("https://www.youtube.com/watch?v={}&t={}s", video_id, m.chunk.start_offset_sec);
                let link = format!("[{} - {}]({})", start_label, end_label, youtube_url);
                let z_str = m.z_score.map(|z| format!("{:.2}", z)).unwrap_or_default();
                println!(
                    "| {} | {} | {} | {} | {} | {} |",
                    rank, link, format_time_fixed(m.chunk.end_offset_sec - m.chunk.start_offset_sec, max_total_secs),
                    m.chunk.unique_authors, m.chunk.total_messages, z_str,
                );
            }
        }
        RankStrategy::MessageRate => {
            println!("| Rank | Time Range | Duration | Unique Authors | Messages | Msg/sec | Z-Score |");
            println!("|------|------------|----------|----------------|----------|---------|---------|");
            for (i, m) in moments.iter().enumerate() {
                let rank = i + 1;
                let start_label = format_time_fixed(m.chunk.start_offset_sec, max_total_secs);
                let end_label = format_time_fixed(m.chunk.end_offset_sec, max_total_secs);
                let youtube_url = format!("https://www.youtube.com/watch?v={}&t={}s", video_id, m.chunk.start_offset_sec);
                let link = format!("[{} - {}]({})", start_label, end_label, youtube_url);
                let rate_str = format!("{:.1}", m.message_rate);
                let z_str = m.z_score.map(|z| format!("{:.2}", z)).unwrap_or_default();
                println!(
                    "| {} | {} | {} | {} | {} | {} | {} |",
                    rank, link, format_time_fixed(m.chunk.end_offset_sec - m.chunk.start_offset_sec, max_total_secs),
                    m.chunk.unique_authors, m.chunk.total_messages, rate_str, z_str,
                );
            }
        }
    }
}

/// Query top N most active chunks for a given video ID.
pub fn print_top_moments(
    db_config: &DbConfig,
    video_id: &str,
    n: usize,
    chunk_duration: i64,
    rank_by: RankStrategy,
    lookback_sec: i64,
    rolling_window: usize,
    members_only: bool,
) -> Result<()> {
    use rusqlite::Connection;

    let conn_path = db_config.connect_path()?;
    let conn = Connection::open(&conn_path).context("Failed to open database")?;

    // Look up the video title (optional — may not be in video_metadata)
    let title: Option<String> = conn
        .query_row(
            "SELECT title FROM video_metadata WHERE video_id = ?",
            [video_id],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("No data for video ID: {}", video_id))?;

    if let Some(ref t) = title {
        println!("Video: {}", t);
    } else {
        eprintln!("Warning: no metadata for video ID: {}", video_id);
    }
    println!("Video ID: {}", video_id);
    if members_only {
        println!("Top {} moments ({} strategy, {} chunks) — members only", n, rank_by, format_time(chunk_duration));
    } else {
        println!("Top {} moments ({} strategy, {} chunks)", n, rank_by, format_time(chunk_duration));
    }
    println!();

    // Get the time range of the video chat
    let (_, max_offset, total_messages): (i64, i64, i64) = conn
        .query_row(
            r#"
            SELECT
                COALESCE(MIN(video_offset_time_msec), 0),
                COALESCE(MAX(video_offset_time_msec), 0),
                COUNT(*)
            FROM live_chat
            WHERE video_id = ?
            "#,
            [video_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map(|opt| opt.unwrap_or((0, 0, 0)))
        .with_context(|| format!("No data for video ID: {}", video_id))?;

    let total_secs = max_offset / 1000;
    println!("Stream duration: {} seconds ({})", total_secs, format_time(total_secs));
    println!("Total messages: {}", total_messages);
    println!("Chunk size: {} seconds ({})", chunk_duration, format_time(chunk_duration));
    println!("Number of chunks: {}", (total_secs / chunk_duration).max(1));
    println!();

    // Load all chunk stats in a single pass
    let chunks = load_chunk_stats(&conn, video_id, chunk_duration, members_only)?;

    if chunks.is_empty() {
        println!("No data available for this video.");
        return Ok(());
    }

    // Pick top moments based on strategy
    let moments: Vec<TopMoment> = match &rank_by {
        RankStrategy::ZScoreUnique => pick_z_score_top(&chunks, n),
        RankStrategy::RollingPeak => pick_rolling_peak_top(&chunks, n, rolling_window, lookback_sec),
        RankStrategy::UniqueAuthors => pick_unique_authors_top(&chunks, n),
        RankStrategy::MessageRate => pick_message_rate_top(&chunks, n),
    };

    // Print results
    print_moments_table(video_id, &moments, &rank_by, total_secs);

    println!();

    // Also print summary statistics
    let all_unique: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT author_channel_id) FROM live_chat WHERE video_id = ?",
            [video_id],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let avg_unique: f64 = if chunks.is_empty() {
        0.0
    } else {
        chunks.iter().map(|c| c.unique_authors as f64).sum::<f64>() / chunks.len() as f64
    };

    let max_unique: usize = chunks.iter().map(|c| c.unique_authors).max().unwrap_or(0);

    println!("--- Summary ---");
    println!("Total unique chatter: {}", all_unique);
    println!("Average unique chatter per chunk: {:.1}", avg_unique);
    println!("Peak unique chatter: {}", max_unique);
    println!(
        "Peak chunk: {} ({})",
        format_time(moments.first().map(|m| m.chunk.start_offset_sec).unwrap_or(0)),
        moments.first()
            .map(|m| format!("{} unique authors", m.chunk.unique_authors))
            .unwrap_or_default()
    );

    // Extra info for rolling-peak
    if rank_by == RankStrategy::RollingPeak {
        if let Some(peak) = moments.first().and_then(|m| m.peak_offset_sec) {
            println!(
                "Peak occurred at: {} ({} unique authors)",
                format_time(peak),
                moments.first().and_then(|m| m.peak_unique).unwrap_or(0)
            );
        }
        println!("Lookback: {} seconds", lookback_sec);
        println!("Rolling window: {} chunks", rolling_window);
    }

    Ok(())
}

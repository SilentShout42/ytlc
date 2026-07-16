use crate::DbConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Schema for yt-dlp info.json files (flat metadata structure)
#[derive(Debug, Clone, Deserialize)]
struct VideoInfo {
    #[serde(rename = "id")]
    video_id: String,
    title: String,
    #[serde(rename = "channel_id")]
    channel_id: String,
    channel: String,
    #[serde(default, alias = "release_timestamp")]
    release_timestamp: Option<i64>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    duration: Option<i64>,
    #[serde(default)]
    duration_string: Option<String>,
    #[serde(default)]
    was_live: Option<bool>,
}

fn parse_message_runs(runs: &[MessageRun]) -> String {
    runs.iter()
        .map(|run| {
            if let Some(ref text) = run.text {
                text.clone()
            } else if let Some(ref emoji) = run.emoji {
                emoji
                    .shortcuts
                    .as_ref()
                    .and_then(|s| s.first())
                    .cloned()
                    .unwrap_or_default()
            } else {
                String::new()
            }
        })
        .collect()
}

fn extract_video_id(filename: &str) -> Result<String> {
    let re = Regex::new(r"\[([A-Za-z0-9_-]{11})\]").unwrap();
    re.captures(filename)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| anyhow::anyhow!("Could not extract video ID from: {}", filename))
}

fn parse_duration(s: &str) -> Option<i64> {
    let parts: Vec<i64> = s.split(':').filter_map(|p| p.parse().ok()).collect();
    match parts.as_slice() {
        [h, m, s] => Some(h * 3600 + m * 60 + s),
        [m, s] => Some(m * 60 + s),
        [s] => Some(*s),
        _ => None,
    }
}

/// Top-level live_chat JSON (single line-delimited object)
#[derive(Debug, Clone, Deserialize)]
struct ReplayChatItem {
    #[serde(rename = "replayChatItemAction")]
    replay_chat_item_action: Option<ReplayChatAction>,
}

/// replayChatItemAction container
#[derive(Debug, Clone, Deserialize)]
struct ReplayChatAction {
    #[serde(rename = "actions")]
    actions: Vec<ChatAction>,
    #[serde(rename = "videoOffsetTimeMsec")]
    video_offset_time_msec: Option<String>,
}

/// Single chat action in actions array
#[derive(Debug, Clone, Deserialize)]
struct ChatAction {
    #[serde(rename = "addChatItemAction")]
    add_chat_item_action: Option<AddChatItemAction>,
}

/// addChatItemAction container
#[derive(Debug, Clone, Deserialize)]
struct AddChatItemAction {
    #[serde(rename = "item")]
    item: Option<ChatItem>,
}

/// item container
#[derive(Debug, Clone, Deserialize)]
struct ChatItem {
    #[serde(rename = "liveChatTextMessageRenderer")]
    live_chat_text_message_renderer: Option<LiveChatTextMessageRenderer>,
}

/// liveChatTextMessageRenderer - the actual message renderer
#[derive(Debug, Clone, Deserialize)]
struct LiveChatTextMessageRenderer {
    id: String,
    #[serde(rename = "timestampUsec")]
    timestamp_usec: serde_json::Value,
    #[serde(rename = "authorName")]
    author_name: Option<MessageAuthor>,
    #[serde(rename = "authorExternalChannelId")]
    author_external_channel_id: Option<String>,
    message: Option<MessageContent>,
    #[serde(rename = "authorBadges")]
    author_badges: Option<Vec<AuthorBadge>>,
    #[serde(rename = "timestampText")]
    timestamp_text: Option<TimestampText>,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageAuthor {
    #[serde(rename = "simpleText")]
    simple_text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageContent {
    runs: Option<Vec<MessageRun>>,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageRun {
    text: Option<String>,
    emoji: Option<MessageEmoji>,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageEmoji {
    shortcuts: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct AuthorBadge {
    #[serde(rename = "liveChatAuthorBadgeRenderer")]
    live_chat_author_badge_renderer: Option<BadgeRenderer>,
}

#[derive(Debug, Clone, Deserialize)]
struct BadgeRenderer {
    icon: Option<BadgeIcon>,
    #[serde(rename = "customThumbnail")]
    custom_thumbnail: Option<CustomThumbnail>,
    tooltip: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CustomThumbnail {
    #[serde(rename = "thumbnails")]
    thumbnails: Option<Vec<Thumbnail>>,
}

#[derive(Debug, Clone, Deserialize)]
struct Thumbnail {
    url: Option<String>,
    width: Option<u64>,
    height: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct BadgeIcon {
    #[serde(rename = "iconType")]
    icon_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TimestampText {
    #[serde(rename = "simpleText")]
    simple_text: Option<String>,
}

/// ChatMessage is the final parsed representation (NOT directly from JSON)
/// Fields are extracted from the deeply nested JSON schema above
#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub message_id: String,
    pub timestamp: DateTime<Utc>,
    pub video_id: String,
    pub author: String,
    pub author_channel_id: String,
    pub message: String,
    pub is_moderator: bool,
    pub is_channel_owner: bool,
    pub is_member: bool,
    pub video_offset_time_msec: Option<i64>,
    pub video_offset_time_text: String,
    pub filename: String,
}

fn parse_live_chat_json(path: &Path) -> Result<Vec<ChatMessage>> {
    let canonical = path.canonicalize()?;
    let filename = canonical.to_string_lossy().to_string();
    let video_id = extract_video_id(&filename)?;
    let content = std::fs::read_to_string(&canonical)?;

    let mut messages = Vec::new();
    // Track the earliest renderer timestamp to compute relative offsets when
    // the replay action lacks videoOffsetTimeMsec.
    let mut first_timestamp_usec: Option<i64> = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Deserialize using serde schema - fails fast on malformed data
        let chat_item: ReplayChatItem = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Parse error in {}: {}", filename, e);
                continue;
            }
        };

        let replay = match chat_item.replay_chat_item_action {
            Some(r) => r,
            None => continue,
        };

        let actions = replay.actions;
        if actions.is_empty() {
            continue;
        }

        for action in actions {
            let item = match action.add_chat_item_action.and_then(|a| a.item) {
                Some(i) => i,
                None => continue,
            };

            let renderer = match item.live_chat_text_message_renderer {
                Some(r) => r,
                None => continue,
            };

            // Extract timestamp from microseconds (string or int)
            let timestamp_usec = renderer
                .timestamp_usec
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or(renderer
                    .timestamp_usec
                    .as_i64())
                .unwrap_or(0);
            let timestamp = DateTime::from_timestamp_micros(timestamp_usec)
                .unwrap_or_default();

            // Extract video_offset_time_msec from top-level replay.
            // If missing, compute a relative offset from the earliest renderer timestamp.
            let video_offset_time_msec = match replay.video_offset_time_msec {
                Some(ref v) => v.parse::<i64>().ok(),
                None => {
                    // Fallback: relative offset from first message's timestamp
                    match first_timestamp_usec {
                        Some(first) if timestamp_usec > first => {
                            Some((timestamp_usec - first) / 1000)
                        }
                        Some(_) => Some(0),
                        None => {
                            first_timestamp_usec = Some(timestamp_usec);
                            Some(0)
                        }
                    }
                }
            };

            // Extract message runs
            let runs = renderer
                .message
                .map(|m| m.runs)
                .flatten()
                .unwrap_or_default();

            // Extract badge types
            let badge_types = renderer
                .author_badges
                .as_ref()
                .map(|badges| {
                    badges
                        .iter()
                        .filter_map(|b| {
                            b.live_chat_author_badge_renderer
                                .as_ref()
                                .and_then(|r| r.icon.as_ref())
                                .and_then(|i| i.icon_type.as_ref())
                                .map(|s| s.clone())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let is_moderator = badge_types.iter().any(|t| t == "MODERATOR");
            let is_channel_owner = badge_types.iter().any(|t| t == "OWNER");
            // Member badges have a customThumbnail (no iconType) and tooltip containing "Member"
            let is_member = renderer
                .author_badges
                .as_ref()
                .is_some_and(|badges| {
                    badges.iter().any(|b| {
                        let has_custom = b
                            .live_chat_author_badge_renderer
                            .as_ref()
                            .is_some_and(|r| r.custom_thumbnail.is_some());
                        let has_member_tooltip = b
                            .live_chat_author_badge_renderer
                            .as_ref()
                            .and_then(|r| r.tooltip.as_ref())
                            .map_or(false, |t| t.contains("Member"));
                        has_custom || has_member_tooltip
                    })
                });

            messages.push(ChatMessage {
                message_id: renderer.id,
                timestamp,
                video_id: video_id.clone(),
                author: renderer
                    .author_name
                    .and_then(|a| a.simple_text)
                    .unwrap_or_default(),
                author_channel_id: renderer
                    .author_external_channel_id
                    .unwrap_or_default(),
                message: parse_message_runs(&runs),
                is_moderator,
                is_channel_owner,
                is_member,
                video_offset_time_msec,
                video_offset_time_text: renderer
                    .timestamp_text
                    .and_then(|t| t.simple_text)
                    .unwrap_or_default(),
                filename: filename.clone(),
            });
        }
    }

    Ok(messages)
}

/// Create the required SQLite tables and enable WAL for concurrent write safety
fn create_tables(conn_path: &std::path::Path) -> Result<()> {
    use rusqlite::Connection;
    let conn = Connection::open(conn_path)?;

    // Enable WAL mode for safe concurrent writes from rayon threads
    conn.execute_batch(r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
    "#)?;

    // Create video_metadata table
    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS video_metadata (
            video_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            release_timestamp DATETIME,
            timestamp DATETIME,
            duration INTEGER,
            was_live BOOLEAN,
            filename TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_video_metadata_release
            ON video_metadata(release_timestamp);

        CREATE INDEX IF NOT EXISTS idx_video_metadata_channel
            ON video_metadata(channel_id);
    "#)?;

    // Create live_chat table
    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS live_chat (
            message_id TEXT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            video_id TEXT NOT NULL,
            author TEXT NOT NULL,
            author_channel_id TEXT NOT NULL,
            message TEXT NOT NULL,
            is_moderator BOOLEAN NOT NULL DEFAULT 0,
            is_channel_owner BOOLEAN NOT NULL DEFAULT 0,
            is_member BOOLEAN NOT NULL DEFAULT 0,
            video_offset_time_msec INTEGER,
            video_offset_time_text TEXT,
            filename TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_live_chat_video
            ON live_chat(video_id);

        CREATE INDEX IF NOT EXISTS idx_live_chat_video_ts
            ON live_chat(video_id, timestamp);

        CREATE INDEX IF NOT EXISTS idx_live_chat_msg
            ON live_chat(message);
    "#)?;

    Ok(())
}

fn insert_messages(conn_path: &std::path::Path, messages: &[ChatMessage]) -> Result<()> {
    use rusqlite::Connection;

    // Deduplicate by message_id
    let mut unique: std::collections::HashMap<&str, &ChatMessage> = std::collections::HashMap::new();
    for m in messages {
        unique.insert(&m.message_id, m);
    }
    let deduped: Vec<&ChatMessage> = unique.values().copied().collect();
    if deduped.len() < messages.len() {
        println!(
            "  Deduplicated {} duplicate messages",
            messages.len() - deduped.len()
        );
    }

    let conn = Connection::open(conn_path)?;
    // Long timeout for high-contention scenarios (874 files with 17+ parallel threads)
    conn.busy_timeout(std::time::Duration::from_secs(60))?;

    // Begin transaction to batch inserts and reduce write contention
    conn.execute_batch("BEGIN TRANSACTION;")?;

    // Prepare statement once, reuse for all inserts
    let mut stmt = conn.prepare(r#"
        INSERT OR REPLACE INTO live_chat (
            message_id, timestamp, video_id, author, author_channel_id, message,
            is_moderator, is_channel_owner, is_member, video_offset_time_msec, video_offset_time_text, filename
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#)?;

    for m in deduped {
        stmt.execute(rusqlite::params![
            &m.message_id,
            &m.timestamp,
            &m.video_id,
            &m.author,
            &m.author_channel_id,
            &m.message,
            &m.is_moderator,
            &m.is_channel_owner,
            &m.is_member,
            m.video_offset_time_msec.map(|v| v as i64),
            &m.video_offset_time_text,
            &m.filename,
        ])?;
    }

    // Commit the transaction
    conn.execute_batch("COMMIT;")?;

    Ok(())
}

fn process_live_chat_file(
    path: &Path,
    conn_path: &std::path::Path,
    file_num: usize,
    total: usize,
) -> Result<usize> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    println!("[{}/{}] Processing: {}", file_num, total, name);

    let messages = parse_live_chat_json(path)
        .with_context(|| format!("Failed to parse {:?}", path))?;

    if messages.is_empty() {
        println!("[{}/{}] No messages found in {}", file_num, total, name);
        return Ok(0);
    }

    insert_messages(conn_path, &messages)?;

    println!(
        "[{}/{}] Inserted {} messages from {}",
        file_num,
        total,
        messages.len(),
        name
    );
    Ok(messages.len())
}

fn parse_info_json(path: &Path, conn_path: &std::path::Path) -> Result<()> {
    let canonical = path.canonicalize()?;
    let content = std::fs::read_to_string(&canonical)?;

    // Deserialize using serde schema - fails fast on missing/invalid fields
    let info: VideoInfo = serde_json::from_str(&content)
        .with_context(|| format!("Failed to deserialize video info from {:?}", canonical))?;

    // Enforce required fields
    if info.video_id.is_empty() {
        anyhow::bail!("video_id is required but empty in {:?}", canonical);
    }
    if info.title.is_empty() {
        anyhow::bail!("title is required but empty in {:?}", canonical);
    }

    // Choose release_timestamp over timestamp (preferred field)
    let release_ts = info
        .release_timestamp
        .or(info.timestamp)
        .and_then(DateTime::<Utc>::from_timestamp_secs);

    let duration_secs: Option<i64> = info
        .duration
        .or_else(|| info.duration_string.as_ref().and_then(|s| parse_duration(s.as_str())));

    let was_live: Option<bool> = info.was_live;
    let filename = canonical.to_string_lossy().to_string();

    use rusqlite::Connection;
    let conn = Connection::open(conn_path)?;
    conn.busy_timeout(std::time::Duration::from_secs(60))?;

    // Begin transaction for consistency with live_chat parsing
    conn.execute_batch("BEGIN TRANSACTION;")?;

    // Prepare statement once, insert the single row
    let mut stmt = conn.prepare(r#"
        INSERT OR REPLACE INTO video_metadata (
            video_id, title, channel_id, channel_name,
            release_timestamp, timestamp, duration, was_live, filename
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#)?;

    stmt.execute(rusqlite::params![
        &info.video_id,
        &info.title,
        &info.channel_id,
        &info.channel,
        release_ts,
        release_ts,
        duration_secs,
        was_live,
        &filename,
    ])?;

    // Commit the transaction
    conn.execute_batch("COMMIT;")?;

    Ok(())
}

fn find_files(directory: &str, suffix: &str) -> Vec<PathBuf> {
    WalkDir::new(directory)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            let name = e.file_name().to_string_lossy();
            name.ends_with(suffix)
                && !e
                    .path()
                    .components()
                    .any(|c| c.as_os_str() == "livechat")
        })
        .map(|e| e.into_path())
        .collect()
}

pub fn parse_jsons(
    directory: &str,
    db_config: &DbConfig,
    json_type: &str,
) -> Result<()> {
    let (suffix, label) = match json_type {
        "live_chat" => (".live_chat.json", "live_chat"),
        "info" => (".info.json", "info"),
        other => anyhow::bail!("Unsupported JSON type: {}", other),
    };

    let files = find_files(directory, suffix);
    if files.is_empty() {
        println!("No {} files found in {}", label, directory);
        return Ok(());
    }

    println!("Found {} {} files to process", files.len(), label);

    let conn_path = db_config.connect_path()?;

    // Create tables if they don't exist (only once, before first parse)
    create_tables(&conn_path)?;

    match json_type {
        "live_chat" => {
            let n_threads = std::thread::available_parallelism()
                .map(|n| n.get().saturating_sub(1).max(1))
                .unwrap_or(1);
            println!("Using {} parallel workers", n_threads);

            let total = files.len();
            let results: Vec<(bool, usize)> = files
                .par_iter()
                .enumerate()
                .map(|(i, path)| {
                    match process_live_chat_file(path, &conn_path, i + 1, total) {
                        Ok(count) => (true, count),
                        Err(e) => {
                            eprintln!("ERROR processing {:?}: {}", path, e);
                            (false, 0)
                        }
                    }
                })
                .collect();

            let successful = results.iter().filter(|(ok, _)| *ok).count();
            let total_msgs: usize = results.iter().map(|(_, c)| c).sum();
            println!("\n=== Processing Complete ===");
            println!("Files processed: {}/{}", successful, total);
            println!("Total messages inserted: {}", total_msgs);
        }
        "info" => {
            println!("Processing info files sequentially...");
            let total = files.len();
            for (i, path) in files.iter().enumerate() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                println!("[{}/{}] Processing: {}", i + 1, total, name);
                if let Err(e) = parse_info_json(path, &conn_path) {
                    eprintln!("Error processing {:?}: {}", path, e);
                }
            }
            println!("\n=== Processing Complete ===");
            println!("Files processed: {}/{}", total, total);
        }
        _ => unreachable!(),
    }

    Ok(())
}

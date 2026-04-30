use crate::DbConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

fn parse_message_runs(runs: &[Value]) -> String {
    runs.iter()
        .map(|run| {
            if let Some(text) = run.get("text").and_then(|t| t.as_str()) {
                text.to_string()
            } else {
                run.get("emoji")
                    .and_then(|e| e.get("shortcuts"))
                    .and_then(|s| s.as_array())
                    .and_then(|a| a.first())
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string()
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

#[derive(Debug)]
struct ChatMessage {
    message_id: String,
    timestamp: DateTime<Utc>,
    video_id: String,
    author: String,
    author_channel_id: String,
    message: String,
    is_moderator: bool,
    is_channel_owner: bool,
    video_offset_time_msec: Option<i64>,
    video_offset_time_text: String,
    filename: String,
}

fn parse_live_chat_json(path: &Path) -> Result<Vec<ChatMessage>> {
    let canonical = path.canonicalize()?;
    let filename = canonical.to_string_lossy().to_string();
    let video_id = extract_video_id(&filename)?;
    let content = std::fs::read_to_string(&canonical)?;

    let mut messages = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let obj: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Parse error in {}: {}", filename, e);
                continue;
            }
        };

        let replay = match obj.get("replayChatItemAction") {
            Some(r) => r,
            None => continue,
        };

        let actions = match replay.get("actions").and_then(|a| a.as_array()) {
            Some(a) => a,
            None => continue,
        };

        for action in actions {
            let renderer = action
                .get("addChatItemAction")
                .and_then(|a| a.get("item"))
                .and_then(|i| i.get("liveChatTextMessageRenderer"));
            let renderer = match renderer {
                Some(r) => r,
                None => continue,
            };

            let runs = renderer
                .get("message")
                .and_then(|m| m.get("runs"))
                .and_then(|r| r.as_array())
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            let timestamp_usec = renderer
                .get("timestampUsec")
                .and_then(|t| t.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| t.as_i64()))
                .unwrap_or(0);
            let timestamp = DateTime::from_timestamp_micros(timestamp_usec)
                .unwrap_or_default();

            let is_badge = |badge_type: &str| {
                renderer
                    .get("authorBadges")
                    .and_then(|b| b.as_array())
                    .map(|badges| {
                        badges.iter().any(|b| {
                            b.get("liveChatAuthorBadgeRenderer")
                                .and_then(|r| r.get("icon"))
                                .and_then(|i| i.get("iconType"))
                                .and_then(|t| t.as_str())
                                == Some(badge_type)
                        })
                    })
                    .unwrap_or(false)
            };

            let video_offset_time_msec = obj
                .get("videoOffsetTimeMsec")
                .or_else(|| replay.get("videoOffsetTimeMsec"))
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });

            messages.push(ChatMessage {
                message_id: renderer
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                timestamp,
                video_id: video_id.clone(),
                author: renderer
                    .get("authorName")
                    .and_then(|a| a.get("simpleText"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                author_channel_id: renderer
                    .get("authorExternalChannelId")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                message: parse_message_runs(runs),
                is_moderator: is_badge("MODERATOR"),
                is_channel_owner: is_badge("OWNER"),
                video_offset_time_msec,
                video_offset_time_text: renderer
                    .get("timestampText")
                    .and_then(|t| t.get("simpleText"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string(),
                filename: filename.clone(),
            });
        }
    }

    Ok(messages)
}

fn insert_messages(client: &mut Client, messages: &[ChatMessage]) -> Result<()> {
    client.execute("SET TIME ZONE 'UTC'", &[])?;

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

    // Collect typed columns for UNNEST batch insert
    let ids: Vec<&str> = deduped.iter().map(|m| m.message_id.as_str()).collect();
    let timestamps: Vec<DateTime<Utc>> = deduped.iter().map(|m| m.timestamp).collect();
    let video_ids: Vec<&str> = deduped.iter().map(|m| m.video_id.as_str()).collect();
    let authors: Vec<&str> = deduped.iter().map(|m| m.author.as_str()).collect();
    let channel_ids: Vec<&str> = deduped.iter().map(|m| m.author_channel_id.as_str()).collect();
    let msgs: Vec<&str> = deduped.iter().map(|m| m.message.as_str()).collect();
    let is_mods: Vec<bool> = deduped.iter().map(|m| m.is_moderator).collect();
    let is_owners: Vec<bool> = deduped.iter().map(|m| m.is_channel_owner).collect();
    let offsets_msec: Vec<Option<i64>> = deduped.iter().map(|m| m.video_offset_time_msec).collect();
    let offset_texts: Vec<&str> = deduped.iter().map(|m| m.video_offset_time_text.as_str()).collect();
    let filenames: Vec<&str> = deduped.iter().map(|m| m.filename.as_str()).collect();

    client.execute(
        r#"
        INSERT INTO live_chat (
            message_id, timestamp, video_id, author, author_channel_id, message,
            is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text, filename
        )
        SELECT * FROM UNNEST(
            $1::text[], $2::timestamptz[], $3::text[], $4::text[], $5::text[], $6::text[],
            $7::bool[], $8::bool[], $9::bigint[], $10::text[], $11::text[]
        )
        ON CONFLICT (message_id) DO UPDATE SET
            timestamp = COALESCE(EXCLUDED.timestamp, live_chat.timestamp),
            video_id = COALESCE(NULLIF(EXCLUDED.video_id, ''), live_chat.video_id),
            author = COALESCE(NULLIF(EXCLUDED.author, ''), live_chat.author),
            author_channel_id = COALESCE(NULLIF(EXCLUDED.author_channel_id, ''), live_chat.author_channel_id),
            message = COALESCE(NULLIF(EXCLUDED.message, ''), live_chat.message),
            is_moderator = COALESCE(EXCLUDED.is_moderator, live_chat.is_moderator),
            is_channel_owner = COALESCE(EXCLUDED.is_channel_owner, live_chat.is_channel_owner),
            video_offset_time_msec = COALESCE(EXCLUDED.video_offset_time_msec, live_chat.video_offset_time_msec),
            video_offset_time_text = COALESCE(NULLIF(EXCLUDED.video_offset_time_text, ''), live_chat.video_offset_time_text),
            filename = COALESCE(NULLIF(EXCLUDED.filename, ''), live_chat.filename)
        "#,
        &[
            &ids, &timestamps, &video_ids, &authors, &channel_ids, &msgs,
            &is_mods, &is_owners, &offsets_msec, &offset_texts, &filenames,
        ],
    )?;

    Ok(())
}

fn process_live_chat_file(
    path: &Path,
    db_config: &DbConfig,
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

    let mut client = Client::connect(&db_config.conn_string(), NoTls)?;
    insert_messages(&mut client, &messages)?;

    println!(
        "[{}/{}] Inserted {} messages from {}",
        file_num,
        total,
        messages.len(),
        name
    );
    Ok(messages.len())
}

fn parse_info_json(path: &Path, db_config: &DbConfig) -> Result<()> {
    let canonical = path.canonicalize()?;
    let content = std::fs::read_to_string(&canonical)?;
    let data: Value = serde_json::from_str(&content)?;

    let video_id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let channel_id = data.get("channel_id").and_then(|v| v.as_str()).unwrap_or("");
    let channel_name = data.get("channel").and_then(|v| v.as_str()).unwrap_or("");

    let release_ts: Option<DateTime<Utc>> = data
        .get("release_timestamp")
        .and_then(|v| v.as_i64())
        .and_then(DateTime::<Utc>::from_timestamp_secs);

    let timestamp: Option<DateTime<Utc>> = data
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .and_then(DateTime::<Utc>::from_timestamp_secs);

    let duration_secs: Option<i64> = data
        .get("duration")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            data.get("duration_string")
                .and_then(|v| v.as_str())
                .and_then(parse_duration)
        });

    let was_live: Option<bool> = data.get("was_live").and_then(|v| v.as_bool());
    let filename = canonical.to_string_lossy().to_string();

    let mut client = Client::connect(&db_config.conn_string(), NoTls)?;
    client.execute("SET TIME ZONE 'UTC'", &[])?;

    client.execute(
        r#"
        INSERT INTO video_metadata (
            video_id, title, channel_id, channel_name,
            release_timestamp, timestamp, duration, was_live, filename
        )
        VALUES ($1, $2, $3, $4, $5, $6,
            $7::bigint * interval '1 second',
            $8, $9)
        ON CONFLICT (video_id) DO UPDATE SET
            title = COALESCE(NULLIF(EXCLUDED.title, ''), video_metadata.title),
            channel_id = COALESCE(NULLIF(EXCLUDED.channel_id, ''), video_metadata.channel_id),
            channel_name = COALESCE(NULLIF(EXCLUDED.channel_name, ''), video_metadata.channel_name),
            release_timestamp = COALESCE(EXCLUDED.release_timestamp, video_metadata.release_timestamp),
            timestamp = COALESCE(EXCLUDED.timestamp, video_metadata.timestamp),
            duration = COALESCE(EXCLUDED.duration, video_metadata.duration),
            was_live = COALESCE(EXCLUDED.was_live, video_metadata.was_live),
            filename = COALESCE(NULLIF(EXCLUDED.filename, ''), video_metadata.filename)
        "#,
        &[
            &video_id,
            &title,
            &channel_id,
            &channel_name,
            &release_ts,
            &timestamp,
            &duration_secs,
            &was_live,
            &filename,
        ],
    )?;

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

pub fn parse_jsons_to_postgres(
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
                    match process_live_chat_file(path, db_config, i + 1, total) {
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
                if let Err(e) = parse_info_json(path, db_config) {
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

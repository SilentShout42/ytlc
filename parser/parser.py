import os
import glob
import orjson
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from multiprocessing import Pool, cpu_count


def parse_message_runs(runs):
    """Parse runs of text and emojis into a single message string."""
    msg = ""
    for run in runs:
        if "text" in run:
            msg += run["text"]
        elif "emoji" in run:
            label = run["emoji"].get("shortcuts", [""])[0]
            msg += label
    return msg


def extract_video_id(filename: str) -> str:
    """Extract the video ID from a filename enclosed in square brackets."""
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


def parse_duration(s: str) -> int | None:
    """Convert a duration string to seconds."""
    if s:
        parts = list(map(int, s.split(":")))
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]
        elif len(parts) == 1:
            return parts[0]
    return None


@dataclass
class ChatMessage:
    message_id: str
    timestamp: datetime
    video_id: str
    author: str
    author_channel_id: str
    message: str
    is_moderator: bool
    is_channel_owner: bool
    video_offset_time_msec: int | None
    video_offset_time_text: str
    filename: str


def parse_live_chat_json(path: str) -> list[ChatMessage]:
    """Parse a YouTube live chat JSONL file and return all messages."""
    canonicalized = os.path.realpath(path)
    video_id = extract_video_id(canonicalized)
    messages = []

    with open(canonicalized, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = orjson.loads(line)
                replay_chat_item_action = obj.get("replayChatItemAction", {})
                if not replay_chat_item_action:
                    continue
                actions = replay_chat_item_action.get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if not renderer:
                        continue

                    # Extract message text
                    runs = renderer.get("message", {}).get("runs", [])
                    msg = parse_message_runs(runs)

                    # Extract timestamp from microseconds
                    ts_usec = int(renderer.get("timestampUsec", 0))
                    timestamp = datetime.fromtimestamp(ts_usec / 1e6, tz=timezone.utc)

                    # Extract author info
                    author = renderer.get("authorName", {}).get("simpleText", "")
                    author_channel_id = renderer.get("authorExternalChannelId", "")
                    message_id = renderer.get("id", "")

                    # Check for badges
                    is_moderator = any(
                        badge.get("liveChatAuthorBadgeRenderer", {})
                        .get("icon", {})
                        .get("iconType", "")
                        == "MODERATOR"
                        for badge in renderer.get("authorBadges", [])
                    )
                    is_channel_owner = any(
                        badge.get("liveChatAuthorBadgeRenderer", {})
                        .get("icon", {})
                        .get("iconType", "")
                        == "OWNER"
                        for badge in renderer.get("authorBadges", [])
                    )

                    # Video offset time
                    video_offset_time_msec = replay_chat_item_action.get("videoOffsetTimeMsec")
                    if video_offset_time_msec is not None:
                        try:
                            video_offset_time_msec = int(video_offset_time_msec)
                        except (ValueError, TypeError):
                            video_offset_time_msec = None

                    video_offset_time_text = renderer.get("timestampText", {}).get("simpleText", "")

                    messages.append(ChatMessage(
                        message_id=message_id,
                        timestamp=timestamp,
                        video_id=video_id,
                        author=author,
                        author_channel_id=author_channel_id,
                        message=msg,
                        is_moderator=is_moderator,
                        is_channel_owner=is_channel_owner,
                        video_offset_time_msec=video_offset_time_msec,
                        video_offset_time_text=video_offset_time_text,
                        filename=canonicalized,
                    ))
            except Exception as e:
                print(f"Error processing line in {canonicalized}: {e}")
                continue

    return messages


def create_tables(db_path: str) -> None:
    """Create the required SQLite tables and enable WAL mode."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 60000;")

    conn.executescript("""
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

        CREATE TABLE IF NOT EXISTS live_chat (
            message_id TEXT PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            video_id TEXT NOT NULL,
            author TEXT NOT NULL,
            author_channel_id TEXT NOT NULL,
            message TEXT NOT NULL,
            is_moderator BOOLEAN NOT NULL DEFAULT 0,
            is_channel_owner BOOLEAN NOT NULL DEFAULT 0,
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
    """)

    conn.close()


def insert_messages_to_sqlite(db_path: str, messages: list[ChatMessage]) -> None:
    """Insert a batch of messages into the SQLite database with deduplication."""
    # Deduplicate by message_id
    unique: dict[str, ChatMessage] = {}
    for m in messages:
        unique[m.message_id] = m
    deduped = list(unique.values())

    if len(deduped) < len(messages):
        print(f"  Deduplicated {len(messages) - len(deduped)} duplicate messages")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 60000;")
    conn.execute("BEGIN TRANSACTION;")

    conn.executemany(
        "INSERT OR REPLACE INTO live_chat "
        "(message_id, timestamp, video_id, author, author_channel_id, message, "
        "is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text, filename) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                m.message_id,
                m.timestamp.isoformat().replace("T", " "),
                m.video_id,
                m.author,
                m.author_channel_id,
                m.message,
                int(m.is_moderator),
                int(m.is_channel_owner),
                m.video_offset_time_msec,
                m.video_offset_time_text,
                m.filename,
            )
            for m in deduped
        ],
    )

    conn.commit()
    conn.close()


def parse_info_json_to_sqlite(path: str, db_path: str) -> None:
    """Parse a YouTube video info JSON file and insert metadata into SQLite."""
    canonicalized = os.path.realpath(path)
    data = orjson.loads(open(canonicalized, "r", encoding="utf-8").read())

    video_id = data.get("id", "")
    if not video_id:
        raise ValueError(f"video_id is required but empty in {canonicalized}")

    title = data.get("title", "")
    if not title:
        raise ValueError(f"title is required but empty in {canonicalized}")

    channel_id = data.get("channel_id", "")
    channel_name = data.get("channel", "")

    # Choose release_timestamp over timestamp (preferred field)
    release_ts_secs = data.get("release_timestamp")
    if release_ts_secs is None:
        release_ts_secs = data.get("timestamp")

    release_ts_str = None
    if release_ts_secs is not None:
        release_ts = datetime.fromtimestamp(int(release_ts_secs), tz=timezone.utc)
        release_ts_str = release_ts.isoformat().replace("T", " ")

    timestamp_str = None
    ts_secs = data.get("timestamp")
    if ts_secs is not None:
        ts = datetime.fromtimestamp(int(ts_secs), tz=timezone.utc)
        timestamp_str = ts.isoformat().replace("T", " ")

    duration = data.get("duration")
    if duration is None:
        duration = parse_duration(data.get("duration_string"))

    was_live = data.get("was_live")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 60000;")
    conn.execute("BEGIN TRANSACTION;")

    conn.execute(
        "INSERT OR REPLACE INTO video_metadata "
        "(video_id, title, channel_id, channel_name, release_timestamp, timestamp, "
        "duration, was_live, filename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            video_id,
            title,
            channel_id,
            channel_name,
            release_ts_str,
            release_ts_str,  # Same as release_timestamp per Rust implementation
            duration,
            was_live,
            canonicalized,
        ),
    )

    conn.commit()
    conn.close()


def process_live_chat_file(args: tuple) -> tuple[int, bool]:
    """Worker function to process a single live chat file (for multiprocessing.Pool)."""
    file_path, db_path, file_num, total = args
    name = Path(file_path).name
    print(f"[{file_num}/{total}] Processing: {name}")

    try:
        messages = parse_live_chat_json(file_path)
        if not messages:
            print(f"[{file_num}/{total}] No messages found in {name}")
            return (file_num, 0, True)

        insert_messages_to_sqlite(db_path, messages)
        print(f"[{file_num}/{total}] Inserted {len(messages)} messages from {name}")
        return (file_num, len(messages), True)
    except Exception as e:
        print(f"[{file_num}/{total}] ERROR processing {file_path}: {e}")
        return (file_num, 0, False)


def find_files(directory: str, suffix: str) -> list[str]:
    """Find all files matching suffix, excluding paths with 'livechat' component."""
    escaped = glob.escape(directory)
    files = glob.glob(f"{escaped}/**/*{suffix}", recursive=True)
    return [f for f in files if "livechat" not in Path(f).parts]


def parse_jsons_to_sqlite(directory: str, db_path: str, json_type: str = "live_chat") -> None:
    """Parse all YouTube JSON files and insert into SQLite.

    For live chat JSONs, messages go into the live_chat table.
    For info JSONs, metadata goes into the video_metadata table.
    """
    if json_type == "live_chat":
        suffix = ".live_chat.json"
        label = "live_chat"
    elif json_type == "info":
        suffix = ".info.json"
        label = "info"
    else:
        print(f"Error: Unsupported JSON type '{json_type}'.")
        return

    files = find_files(directory, suffix)
    if not files:
        print(f"No {label} files found in {directory}")
        return

    print(f"Found {len(files)} {label} files to process")

    # Create tables once before processing
    create_tables(db_path)

    if json_type == "live_chat":
        n_threads = max(1, cpu_count() - 1)
        print(f"Using {n_threads} parallel workers")

        total = len(files)
        worker_args = [
            (file_path, db_path, i + 1, total)
            for i, file_path in enumerate(files)
        ]

        with Pool(processes=n_threads) as pool:
            results = pool.map(process_live_chat_file, worker_args)

        successful = sum(1 for _, _, success in results if success)
        total_messages = sum(count for _, count, success in results if success)
        print(f"\n=== Processing Complete ===")
        print(f"Files processed: {successful}/{total}")
        print(f"Total messages inserted: {total_messages}")

    else:
        print("Processing info files sequentially...")
        for i, file_path in enumerate(files, 1):
            name = Path(file_path).name
            print(f"[{i}/{len(files)}] Processing: {name}")
            try:
                parse_info_json_to_sqlite(file_path, db_path)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

        print(f"\n=== Processing Complete ===")
        print(f"Files processed: {len(files)}/{len(files)}")

import json
import csv
import os
import pandas as pd
import sqlite3
import glob
import re
from collections import defaultdict


class LiveChatMessage:
    def __init__(self, timestamp_usec, video_id, video_offset_time_seconds, message):
        self.timestamp_usec = timestamp_usec
        self.video_id = video_id
        self.video_offset_time_seconds = video_offset_time_seconds
        self.message = message


class VideoMetadata:
    def __init__(self, video_id, title, channel_id, channel_name, release_timestamp):
        self.video_id = video_id
        self.title = title
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.release_timestamp = release_timestamp


def parse_live_chat_json_to_sqlite(json_path, db_path="chat_messages.db"):
    """
    Parses a YouTube live chat JSONL file and inserts messages into a SQLite database.
    Messages are stored in a single table named `live_chat`.
    Ensures duplicates are not inserted when re-processing a file.
    """
    # Extract video ID from the JSON filename
    video_id = extract_video_id_from_filename(json_path)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    conn.text_factory = str  # Ensure support for international characters
    cursor = conn.cursor()

    # Set SQLite pragmas for performance and reliability
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA journal_size_limit=10485760;")  # 10 MB

    # Create the table if it doesn't exist
    table_name = "live_chat"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp_usec INTEGER PRIMARY KEY,
            timestamp_text TEXT,
            video_id TEXT,
            author TEXT,
            author_channel_id TEXT,
            message TEXT,
            is_moderator BOOLEAN,
            is_channel_owner BOOLEAN,
            video_offset_time_msec INTEGER,
            video_offset_time_text TEXT
        )
        """
    )

    # Begin a transaction for batch writes
    conn.execute("BEGIN TRANSACTION;")

    with open(json_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = json.loads(line)
                actions = obj.get("replayChatItemAction", {}).get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if renderer:
                        # Extract message text (concatenate all runs)
                        runs = renderer.get("message", {}).get("runs", [])
                        msg = ""
                        for run in runs:
                            if "text" in run:
                                msg += run["text"]
                            elif "emoji" in run:
                                label = run["emoji"].get("shortcuts", [""])[0]
                                msg += label
                        timestamp_usec = int(renderer.get("timestampUsec", "0"))
                        # Convert timestamp_usec to ISO 8601 format with time (YYYY-MM-DD HH:MM:SS.SSSZ)
                        timestamp_iso = f"{pd.to_datetime(timestamp_usec, unit='us').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}Z"
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        author_channel_id = renderer.get("authorExternalChannelId", "")

                        # Check for moderator and channel owner badges
                        is_moderator = False
                        is_channel_owner = False
                        for badge in renderer.get("authorBadges", []):
                            badge_renderer = badge.get(
                                "liveChatAuthorBadgeRenderer", {}
                            )
                            icon_type = badge_renderer.get("icon", {}).get(
                                "iconType", ""
                            )
                            if icon_type == "MODERATOR":
                                is_moderator = True
                            elif icon_type == "OWNER":
                                is_channel_owner = True

                        # Extract videoOffsetTimeMsec for each chat message
                        video_offset_time_msec = obj.get("replayChatItemAction", {}).get("videoOffsetTimeMsec", None)

                        # Extract videoOffsetText for each chat message
                        video_offset_time_text = renderer.get("timestampText", {}).get("simpleText", "")

                        # Insert the message into the database, including video_offset_time_msec and video_offset_time_text
                        cursor.execute(
                            f"""
                            INSERT OR IGNORE INTO {table_name} (timestamp_usec, timestamp_text, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                timestamp_usec,
                                timestamp_iso,
                                video_id,
                                author,
                                author_channel_id,
                                msg,
                                is_moderator,
                                is_channel_owner,
                                video_offset_time_msec,
                                video_offset_time_text,
                            ),
                        )
            except Exception as e:
                continue

    # Commit the transaction
    conn.commit()
    conn.close()


def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


def search_messages_in_database(db_path, regex_pattern, window_size=60, min_matches=5):
    """
    Searches all videos in the SQLite database for messages matching a specific regex pattern.
    Prints the first matching message from each satisfactory time window as YouTube links.

    Parameters:
        db_path (str): Path to the SQLite database.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
    """
    import re

    # Read messages from the database into a pandas DataFrame
    query = "SELECT timestamp_usec, video_id, video_offset_time_msec, message FROM live_chat;"
    df = pd.read_sql_query(query, sqlite3.connect(db_path))

    # Ensure video_offset_time_msec is an integer
    df['video_offset_time_msec'] = df['video_offset_time_msec'].fillna(0).astype(int)

    # Convert timestamp_usec to datetime for easier processing
    df['timestamp'] = pd.to_datetime(df['timestamp_usec'], unit='us')

    # Filter rows matching the regex pattern
    pattern = re.compile(regex_pattern)
    df['matches'] = df['message'].apply(lambda x: bool(pattern.search(x)))
    matching_df = df[df['matches']]

    # Group by video_id and time window
    matching_df['time_window'] = matching_df['timestamp'].dt.floor(f'{window_size}s')
    grouped = matching_df.groupby(['video_id', 'time_window'])

    # Filter groups with at least the required number of matches
    results = []
    for (video_id, time_window), group in grouped:
        if len(group) >= min_matches:
            first_message = group.iloc[0]
            results.append({
                'video_id': video_id,
                'time_window': time_window,
                'message': first_message['message'],
                'video_offset_time_seconds': first_message['video_offset_time_msec'] // 1000,
                'timestamp_usec': first_message['timestamp_usec']
            })

    # Sort results by timestamp_usec (oldest to newest)
    results = sorted(results, key=lambda x: x['timestamp_usec'])

    # Print matching messages as YouTube links along with the message text
    for result in results:
        link = f"https://www.youtube.com/watch?v={result['video_id']}&t={result['video_offset_time_seconds']}s"
        print(f"{link} - {result['message']}")


def parse_jsons_to_sqlite(
    directory_path, db_path="chat_messages.db", json_type="live_chat"
):
    """
    Parses all YouTube JSON files (live chat or info) in a directory tree and inserts data into a SQLite database.
    For live chat JSONs, messages are stored in a single table named `live_chat`.
    For info JSONs, metadata is stored in a table named `video_metadata`.
    """
    # Ensure the directory exists
    if not os.path.exists(directory_path):
        print(f"Error: Directory '{directory_path}' does not exist.")
        return

    # Escape the directory path for glob
    escaped_path = glob.escape(directory_path)

    # Determine file pattern and processing logic based on json_type
    if json_type == "live_chat":
        file_pattern = "**/*.live_chat.json"
        process_function = parse_live_chat_json_to_sqlite
    elif json_type == "info":
        file_pattern = "**/*.info.json"
        process_function = parse_info_json_to_sqlite
    else:
        print(f"Error: Unsupported JSON type '{json_type}'.")
        return

    # Find all matching JSON files in the directory tree
    json_files = glob.glob(f"{escaped_path}/{file_pattern}", recursive=True)

    for json_file in json_files:
        process_function(json_file, db_path)

    print(
        f"Processed {len(json_files)} {json_type} files into the SQLite database: {db_path}"
    )


def parse_info_json_to_sqlite(json_path, db_path="chat_messages.db"):
    """
    Parses a YouTube video info JSON file and inserts metadata into a SQLite database.
    Creates a table called "video_metadata" with columns: video_id, title, channel_id, channel_name, release_timestamp.
    Stores release_timestamp in the format YYYY-MM-DD HH:MM:SS.SSSZ using the `timestamp` field.
    """
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    conn.text_factory = str  # Ensure support for international characters
    cursor = conn.cursor()

    # Set SQLite pragmas for performance and reliability
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA journal_size_limit=10485760;")  # Set journal size limit to 10 MB

    # Create the video_metadata table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS video_metadata (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            channel_id TEXT,
            channel_name TEXT,
            release_timestamp TEXT
        )
        """
    )

    try:
        with open(json_path, "r", encoding="utf-8") as infile:
            data = json.load(infile)

            # Extract required fields
            video_id = data.get("id", "")
            title = data.get("title", "")
            channel_id = data.get("channel_id", "")
            channel_name = data.get("channel", "")
            timestamp = data.get("timestamp", None)

            # Convert timestamp to ISO 8601 format with time (YYYY-MM-DD HH:MM:SS.SSSZ)
            release_timestamp = ""
            if timestamp:
                release_timestamp = f"{pd.to_datetime(timestamp, unit='s').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}Z"

            # Insert metadata into the database
            cursor.execute(
                """
                INSERT OR IGNORE INTO video_metadata (video_id, title, channel_id, channel_name, release_timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (video_id, title, channel_id, channel_name, release_timestamp),
            )
    except Exception as e:
        print(f"Error processing file {json_path}: {e}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def main():
    directory_path = r"/home/localuser/mnt/media/youtube/out/Kanna_Yanagi_ch._[UClxj3GlGphZVgd1SLYhZKmg]"
    db_path = "chat_messages.db"
    # parse_jsons_to_sqlite(directory_path, db_path, json_type="info")
    # parse_jsons_to_sqlite(directory_path, db_path, json_type="live_chat")
    search_messages_in_database(db_path, r"(?i)^(?=.*bless you)(?!.*god).*$")


if __name__ == "__main__":
    main()

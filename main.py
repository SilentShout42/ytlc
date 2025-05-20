import json
import csv
import os
import pandas as pd
import sqlite3
import glob
import re
from collections import defaultdict
import orjson
import psycopg2
from psycopg2.extras import execute_values
import time
import asyncio

# Enable pandas copy-on-write mode for memory optimization
pd.options.mode.copy_on_write = True


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


def parse_message_runs(runs):
    """
    Parses runs of text and emojis into a single message string.

    Parameters:
        runs (list): List of runs containing text and/or emojis.

    Returns:
        str: Concatenated message string.
    """
    msg = ""
    for run in runs:
        if "text" in run:
            msg += run["text"]
        elif "emoji" in run:
            label = run["emoji"].get("shortcuts", [""])[0]
            msg += label
    return msg


def parse_live_chat_json(json_path):
    """
    Parses a YouTube live chat JSONL file and extracts messages.

    Returns:
        list: A list of dictionaries containing parsed message data.
    """
    messages = []
    video_id = extract_video_id_from_filename(json_path)

    with open(json_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = orjson.loads(line)
                actions = obj.get("replayChatItemAction", {}).get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if renderer:
                        # Extract message text (concatenate all runs)
                        runs = renderer.get("message", {}).get("runs", [])
                        msg = parse_message_runs(runs)
                        timestamp_usec = int(renderer.get("timestampUsec", "0"))
                        timestamp_iso = f"{pd.to_datetime(timestamp_usec, unit='us').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}Z"
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        author_channel_id = renderer.get("authorExternalChannelId", "")
                        message_id = renderer.get("id", "")

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

                        video_offset_time_msec = obj.get(
                            "replayChatItemAction", {}
                        ).get("videoOffsetTimeMsec", None)
                        video_offset_time_text = renderer.get("timestampText", {}).get(
                            "simpleText", ""
                        )

                        messages.append(
                            {
                                "message_id": message_id,
                                "timestamp_usec": timestamp_usec,
                                "timestamp_iso": timestamp_iso,
                                "video_id": video_id,
                                "author": author,
                                "author_channel_id": author_channel_id,
                                "message": msg,
                                "is_moderator": is_moderator,
                                "is_channel_owner": is_channel_owner,
                                "video_offset_time_msec": video_offset_time_msec,
                                "video_offset_time_text": video_offset_time_text,
                            }
                        )
            except Exception as e:
                pass  # Placeholder to fix indentation

    return messages


def parse_live_chat_json_buffered(json_path, buffer_size=10000):
    """
    Parses a YouTube live chat JSONL file and yields messages in buffered batches.

    Parameters:
        json_path (str): Path to the JSONL file.
        buffer_size (int): Number of messages to buffer before yielding.

    Yields:
        list: A list of buffered messages.
    """
    buffer = []
    video_id = extract_video_id_from_filename(json_path)

    with open(json_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = orjson.loads(line)
                actions = obj.get("replayChatItemAction", {}).get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if renderer:
                        # Extract message text (concatenate all runs)
                        runs = renderer.get("message", {}).get("runs", [])
                        msg = parse_message_runs(runs)
                        timestamp_usec = int(renderer.get("timestampUsec", "0"))
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        author_channel_id = renderer.get("authorExternalChannelId", "")
                        message_id = renderer.get("id", "")

                        # Check for moderator and channel owner badges
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

                        video_offset_time_msec = obj.get(
                            "replayChatItemAction", {}
                        ).get("videoOffsetTimeMsec", None)
                        video_offset_time_text = renderer.get("timestampText", {}).get(
                            "simpleText", ""
                        )

                        buffer.append(
                            {
                                "message_id": message_id,
                                "timestamp_usec": timestamp_usec,
                                "video_id": video_id,
                                "author": author,
                                "author_channel_id": author_channel_id,
                                "message": msg,
                                "is_moderator": is_moderator,
                                "is_channel_owner": is_channel_owner,
                                "video_offset_time_msec": video_offset_time_msec,
                                "video_offset_time_text": video_offset_time_text,
                            }
                        )

                        if len(buffer) >= buffer_size:
                            yield buffer
                            buffer = []
            except Exception:
                pass  # Ignore errors for now

    if buffer:
        yield buffer


def parse_live_chat_json_to_sqlite(json_path, db_path="chat_messages.db"):
    """
    Parses a YouTube live chat JSONL file and inserts messages into a SQLite database.
    Messages are stored in a single table named `live_chat`.
    Ensures duplicates are not inserted when re-processing a file.
    """
    messages = parse_live_chat_json(json_path)

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
            message_id TEXT PRIMARY KEY,
            timestamp_usec INTEGER,
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

    for message in messages:
        cursor.execute(
            f"""
            INSERT OR IGNORE INTO {table_name} (message_id, timestamp_usec, timestamp_text, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message["message_id"],
                message["timestamp_usec"],
                message["timestamp_iso"],
                message["video_id"],
                message["author"],
                message["author_channel_id"],
                message["message"],
                message["is_moderator"],
                message["is_channel_owner"],
                message["video_offset_time_msec"],
                message["video_offset_time_text"],
            ),
        )

    # Commit the transaction
    conn.commit()
    conn.close()


def insert_messages_to_postgres(messages, db_config):
    """
    Inserts a batch of messages into the PostgreSQL database.

    Parameters:
        messages (list): List of message dictionaries to insert.
        db_config (dict): Database configuration for PostgreSQL connection.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    table_name = "live_chat"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            message_id TEXT PRIMARY KEY,
            timestamp_usec BIGINT,
            timestamp_text TIMESTAMP,
            video_id TEXT,
            author TEXT,
            author_channel_id TEXT,
            message TEXT,
            is_moderator BOOLEAN,
            is_channel_owner BOOLEAN,
            video_offset_time_msec BIGINT,
            video_offset_time_text TEXT
        )
        """
    )

    execute_values(
        cursor,
        f"""
        INSERT INTO {table_name} (message_id, timestamp_usec, timestamp_text, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text)
        VALUES %s ON CONFLICT (message_id) DO NOTHING
        """,
        [
            (
                message["message_id"],
                message["timestamp_usec"],
                pd.to_datetime(message["timestamp_usec"], unit="us", utc=True),
                message["video_id"],
                message["author"],
                message["author_channel_id"],
                message["message"],
                message["is_moderator"],
                message["is_channel_owner"],
                message["video_offset_time_msec"],
                message["video_offset_time_text"],
            )
            for message in messages
        ],
    )

    conn.commit()
    conn.close()


async def async_insert_messages_to_postgres(messages, db_config):
    """
    Asynchronously inserts a batch of messages into the PostgreSQL database.

    Parameters:
        messages (list): List of message dictionaries to insert.
        db_config (dict): Database configuration for PostgreSQL connection.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    table_name = "live_chat"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            message_id TEXT PRIMARY KEY,
            timestamp_usec BIGINT,
            timestamp_text TIMESTAMP,
            video_id TEXT,
            author TEXT,
            author_channel_id TEXT,
            message TEXT,
            is_moderator BOOLEAN,
            is_channel_owner BOOLEAN,
            video_offset_time_msec BIGINT,
            video_offset_time_text TEXT
        )
        """
    )

    execute_values(
        cursor,
        f"""
        INSERT INTO {table_name} (message_id, timestamp_usec, timestamp_text, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text)
        VALUES %s ON CONFLICT (message_id) DO NOTHING
        """,
        [
            (
                message["message_id"],
                message["timestamp_usec"],
                pd.to_datetime(message["timestamp_usec"], unit="us", utc=True),
                message["video_id"],
                message["author"],
                message["author_channel_id"],
                message["message"],
                message["is_moderator"],
                message["is_channel_owner"],
                message["video_offset_time_msec"],
                message["video_offset_time_text"],
            )
            for message in messages
        ],
    )

    conn.commit()
    conn.close()


async def async_parse_live_chat_json_buffered(json_path, buffer_size=10000):
    """
    Asynchronously parses a YouTube live chat JSONL file and yields messages in buffered batches.

    Parameters:
        json_path (str): Path to the JSONL file.
        buffer_size (int): Number of messages to buffer before yielding.

    Yields:
        list: A list of buffered messages.
    """
    buffer = []
    video_id = extract_video_id_from_filename(json_path)

    with open(json_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = orjson.loads(line)
                actions = obj.get("replayChatItemAction", {}).get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if renderer:
                        # Extract message text (concatenate all runs)
                        runs = renderer.get("message", {}).get("runs", [])
                        msg = parse_message_runs(runs)
                        timestamp_usec = int(renderer.get("timestampUsec", "0"))
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        author_channel_id = renderer.get("authorExternalChannelId", "")
                        message_id = renderer.get("id", "")

                        # Check for moderator and channel owner badges
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

                        video_offset_time_msec = obj.get(
                            "replayChatItemAction", {}
                        ).get("videoOffsetTimeMsec", None)
                        video_offset_time_text = renderer.get("timestampText", {}).get(
                            "simpleText", ""
                        )

                        buffer.append(
                            {
                                "message_id": message_id,
                                "timestamp_usec": timestamp_usec,
                                "video_id": video_id,
                                "author": author,
                                "author_channel_id": author_channel_id,
                                "message": msg,
                                "is_moderator": is_moderator,
                                "is_channel_owner": is_channel_owner,
                                "video_offset_time_msec": video_offset_time_msec,
                                "video_offset_time_text": video_offset_time_text,
                            }
                        )

                        if len(buffer) >= buffer_size:
                            yield buffer
                            buffer = []
            except Exception:
                pass  # Ignore errors for now

    if buffer:
        yield buffer


async def async_parse_and_insert(file_path, db_config, buffer, buffer_size):
    """
    Asynchronously parses a single file and inserts messages into PostgreSQL.

    Parameters:
        file_path (str): Path to the JSON file to process.
        db_config (dict): Database configuration for PostgreSQL connection.
        buffer (list): Shared buffer to accumulate messages.
        buffer_size (int): Number of messages to buffer before inserting.
    """
    async for batch in async_parse_live_chat_json_buffered(file_path, buffer_size):
        buffer.extend(batch)

        if len(buffer) >= buffer_size:
            await commit_buffer_to_postgres(buffer, db_config)


async def commit_buffer_to_postgres(buffer, db_config):
    """
    Commits the accumulated buffer to PostgreSQL and clears the buffer.

    Parameters:
        buffer (list): Shared buffer to accumulate messages.
        db_config (dict): Database configuration for PostgreSQL connection.
    """
    if buffer:
        print(f"Committing {len(buffer)} messages to the database.")
        await async_insert_messages_to_postgres(buffer, db_config)
        buffer.clear()


async def process_files_to_postgres_async(file_paths, db_config, buffer_size=10000):
    """
    Asynchronously processes multiple JSON files, buffering messages and inserting them into PostgreSQL.

    Parameters:
        file_paths (list): List of file paths to process.
        db_config (dict): Database configuration for PostgreSQL connection.
        buffer_size (int): Number of messages to buffer before inserting.
    """
    buffer = []
    for file_path in file_paths:
        await async_parse_and_insert(file_path, db_config, buffer, buffer_size)

    # Commit any remaining messages in the buffer
    await commit_buffer_to_postgres(buffer, db_config)


def process_files_to_postgres(file_paths, db_config, buffer_size=10000):
    """
    Processes multiple JSON files, buffering messages and inserting them into PostgreSQL.

    Parameters:
        file_paths (list): List of file paths to process.
        db_config (dict): Database configuration for PostgreSQL connection.
        buffer_size (int): Number of messages to buffer before inserting.
    """
    total_messages = 0
    total_parse_time = 0
    total_insert_time = 0
    total_buffer_fill_time = 0  # Initialize total buffer fill time
    buffer = []
    buffer_start_time = time.time()

    for file_path in file_paths:
        for batch in parse_live_chat_json_buffered(file_path, buffer_size):
            buffer.extend(batch)

            if len(buffer) >= buffer_size:
                # Log buffer fill time
                buffer_fill_time = time.time() - buffer_start_time
                total_buffer_fill_time += (
                    buffer_fill_time  # Accumulate buffer fill time
                )
                print(
                    f"Buffer filled with {len(buffer)} messages in {buffer_fill_time:.2f} seconds."
                )

                # Insert messages into the database
                insert_start_time = time.time()
                insert_messages_to_postgres(buffer, db_config)
                insert_time = time.time() - insert_start_time
                print(
                    f"Inserted {len(buffer)} messages into the database in {insert_time:.2f} seconds."
                )

                total_insert_time += insert_time
                total_messages += len(buffer)
                buffer = []  # Clear the buffer
                buffer_start_time = time.time()  # Reset buffer start time

    # Flush remaining messages in the buffer
    if buffer:
        buffer_fill_time = time.time() - buffer_start_time
        total_buffer_fill_time += buffer_fill_time  # Accumulate buffer fill time
        print(
            f"Flushing remaining {len(buffer)} messages after {buffer_fill_time:.2f} seconds."
        )

        insert_start_time = time.time()
        insert_messages_to_postgres(buffer, db_config)
        insert_time = time.time() - insert_start_time
        print(
            f"Inserted {len(buffer)} messages into the database in {insert_time:.2f} seconds."
        )

        total_insert_time += insert_time
        total_messages += len(buffer)

    print(
        f"Processed {total_messages} messages in total. Total buffer fill time: {total_buffer_fill_time:.2f} seconds. Total insert time: {total_insert_time:.2f} seconds."
    )


def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


def search_messages(db_path, regex_pattern, window_size=60, min_matches=5):
    """
    Searches the database for messages matching a regex pattern and groups them by video ID and time window.

    Parameters:
        db_path (str): Path to the SQLite database.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.

    Returns:
        list: A list of dictionaries containing grouped search results.
    """
    import re

    # Read messages and metadata from the database into a pandas DataFrame
    query = """
        SELECT
            lc.timestamp_usec,
            lc.video_id,
            lc.video_offset_time_msec,
            lc.message,
            vm.release_timestamp,
            vm.title
        FROM live_chat lc
        JOIN video_metadata vm ON lc.video_id = vm.video_id;
    """
    df = pd.read_sql_query(query, sqlite3.connect(db_path))

    # Ensure video_offset_time_msec is an integer
    df["video_offset_time_msec"] = df["video_offset_time_msec"].fillna(0).astype(int)

    # Convert timestamp_usec to datetime for easier processing
    df["timestamp"] = pd.to_datetime(df["timestamp_usec"], unit="us")

    # Filter rows matching the regex pattern
    pattern = re.compile(regex_pattern)
    df["matches"] = df["message"].apply(lambda x: bool(pattern.search(x)))
    matching_df = df[df["matches"]]

    # Group by video_id and time window
    matching_df["time_window"] = matching_df["timestamp"].dt.floor(f"{window_size}s")
    grouped = matching_df.groupby(["video_id", "time_window"])

    # Filter groups with at least the required number of matches
    results = []
    for (video_id, time_window), group in grouped:
        if len(group) >= min_matches:
            first_message = group.iloc[0]
            results.append(
                {
                    "video_id": video_id,
                    "video_date": pd.to_datetime(
                        first_message["release_timestamp"]
                    ).strftime("%Y-%m-%d"),
                    "video_title": first_message["title"],
                    "video_offset_time_seconds": first_message["video_offset_time_msec"]
                    // 1000,
                    "timestamp_usec": first_message["timestamp_usec"],
                    "message": first_message["message"],
                }
            )

    # Sort results by timestamp_usec (oldest to newest)
    return sorted(results, key=lambda x: x["timestamp_usec"])


def print_search_results_as_markdown(
    db_path, regex_pattern, window_size=60, min_matches=5, timestamp_offset=10
):
    """
    Searches the database and prints results as a markdown table with columns:
    - Video Date (YYYY-mm-dd)
    - Video Title (as a YouTube link)
    - Timestamp Link (HH:MM:SS)
    - Message Text

    Parameters:
        db_path (str): Path to the SQLite database.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
        timestamp_offset (int): Number of seconds to subtract from the timestamp for context.
    """
    results = search_messages(db_path, regex_pattern, window_size, min_matches)

    # Print results as a markdown table
    print("| Video Date | Video Title | Timestamp Link | Message Text |")
    print("|------------|-------------|----------------|--------------|")
    for result in results:
        video_link = f"https://www.youtube.com/watch?v={result['video_id']}"
        timestamp_adjusted_seconds = max(
            result["video_offset_time_seconds"] - timestamp_offset, 0
        )
        timestamp_link = f"{video_link}&t={timestamp_adjusted_seconds}s"
        timestamp_hms = pd.to_datetime(timestamp_adjusted_seconds, unit="s").strftime(
            "%H:%M:%S"
        )
        print(
            f"| {result['video_date']} | [**{result['video_title']}**]({video_link}) | [**{timestamp_hms}**]({timestamp_link}) | {result['message']} |"
        )


def generate_sortable_html_table(
    db_path,
    regex_pattern,
    window_size=60,
    min_matches=5,
    output_file="results.html",
    timestamp_offset=10,
):
    """
    Searches the database and generates a sortable HTML table with columns:
    - Video Date (YYYY-mm-dd)
    - Video Title (as a YouTube link)
    - Timestamp Link (HH:MM:SS)
    - Message Text
    The table is saved to an HTML file for publishing.

    Parameters:
        db_path (str): Path to the SQLite database.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
        output_file (str): Path to save the generated HTML file.
        timestamp_offset (int): Number of seconds to subtract from the timestamp for context.
    """
    results = search_messages(db_path, regex_pattern, window_size, min_matches)

    # Generate HTML table
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Search Results</title>
        <style>
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                cursor: pointer;
                background-color: #f2f2f2;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            tr:hover {
                background-color: #f1f1f1;
            }
        </style>
        <script>
            function sortTable(n) {
                const table = document.getElementById("resultsTable");
                let rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                switching = true;
                dir = "asc";
                while (switching) {
                    switching = false;
                    rows = table.rows;
                    for (i = 1; i < (rows.length - 1); i++) {
                        shouldSwitch = false;
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        if (dir === "asc") {
                            if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                                shouldSwitch = true;
                                break;
                            }
                        } else if (dir === "desc") {
                            if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                    }
                    if (shouldSwitch) {
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount++;
                    } else {
                        if (switchcount === 0 && dir === "asc") {
                            dir = "desc";
                            switching = true;
                        }
                    }
                }
            }
        </script>
    </head>
    <body>
        <h1>Search Results</h1>
        <table id="resultsTable">
            <thead>
                <tr>
                    <th onclick="sortTable(0)">Video Date</th>
                    <th onclick="sortTable(1)">Video Title</th>
                    <th onclick="sortTable(2)">Timestamp Link</th>
                    <th onclick="sortTable(3)">Message Text</th>
                </tr>
            </thead>
            <tbody>
    """

    for result in results:
        video_link = f"https://www.youtube.com/watch?v={result['video_id']}"
        timestamp_adjusted_seconds = max(
            result["video_offset_time_seconds"] - timestamp_offset, 0
        )
        timestamp_link = f"{video_link}&t={timestamp_adjusted_seconds}s"
        timestamp_hms = pd.to_datetime(timestamp_adjusted_seconds, unit="s").strftime(
            "%H:%M:%S"
        )
        html += f"<tr>"
        html += f"<td>{result['video_date']}</td>"
        html += f"<td><a href='{video_link}' target='_blank'>{result['video_title']}</a></td>"
        html += (
            f"<td><a href='{timestamp_link}' target='_blank'>{timestamp_hms}</a></td>"
        )
        html += f"<td>{result['message']}</td>"
        html += f"</tr>"

    html += """
            </tbody>
        </table>
    </body>
    </html>
    """

    # Save HTML to file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Results saved to {output_file}")


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


def parse_jsons_to_postgres(directory_path, db_config, json_type="live_chat"):
    """
    Parses all YouTube JSON files (live chat or info) in a directory tree and inserts data into a PostgreSQL database.
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
        process_function = parse_live_chat_json_buffered
    elif json_type == "info":
        file_pattern = "**/*.info.json"
        process_function = parse_info_json_to_postgres
    else:
        print(f"Error: Unsupported JSON type '{json_type}'.")
        return

    # Find all matching JSON files in the directory tree
    json_files = glob.glob(f"{escaped_path}/{file_pattern}", recursive=True)

    if json_type == "live_chat":
        asyncio.run(process_files_to_postgres_async(json_files, db_config))
    else:
        for json_file in json_files:
            process_function(json_file, db_config)

    print(
        f"Processed {len(json_files)} {json_type} files into the PostgreSQL database."
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
    cursor.execute(
        "PRAGMA journal_size_limit=10485760;"
    )  # Set journal size limit to 10 MB

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
            data = orjson.loads(infile.read())
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


def parse_info_json_to_postgres(json_path, db_config):
    """
    Parses a YouTube video info JSON file and inserts metadata into a PostgreSQL database.
    Creates a table called "video_metadata" with columns: video_id, title, channel_id, channel_name, release_timestamp.
    """
    # Connect to PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the video_metadata table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS video_metadata (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            channel_id TEXT,
            channel_name TEXT,
            release_timestamp TIMESTAMP
        )
        """
    )

    try:
        with open(json_path, "r", encoding="utf-8") as infile:
            data = orjson.loads(infile.read())
            # Extract required fields
            video_id = data.get("id", "")
            title = data.get("title", "")
            channel_id = data.get("channel_id", "")
            channel_name = data.get("channel", "")
            timestamp = data.get("timestamp", None)

            # Convert timestamp to native PostgreSQL TIMESTAMP format in UTC
            release_timestamp = None
            if timestamp:
                release_timestamp = pd.to_datetime(timestamp, unit="s", utc=True)

            # Insert metadata into the database
            cursor.execute(
                """
                INSERT INTO video_metadata (video_id, title, channel_id, channel_name, release_timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING
                """,
                (video_id, title, channel_id, channel_name, release_timestamp),
            )
    except Exception as e:
        print(f"Error processing file {json_path}: {e}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def main():
    db_config = {
        "dbname": "ytlc",
        "user": "ytlc",
        "host": "localhost",
        "port": 5432,
    }
    directory_path = r"/home/wsluser/mnt/media/youtube/out/Kanna_Yanagi_ch._[UClxj3GlGphZVgd1SLYhZKmg]/2025/05"
    parse_jsons_to_postgres(directory_path, db_config, json_type="info")
    # parse_jsons_to_postgres(directory_path, db_config, json_type="live_chat")
    # search_messages_in_database(db_path, r"(?i)^(?=.*bless you)(?!.*god).*$")
    # search_messages_in_database(db_path, r"(?i)bless you(?! [^!:k])")
    # print_search_results_as_markdown(db_path, r"(?i)bless you(?! [^!:k])")
    # generate_sortable_html_table(db_path, r"(?i)bless you(?! [^!:k])", window_size=120)
    # generate_sortable_html_table(db_path, r"(?i)tskr", window_size=120, timestamp_offset=15)


if __name__ == "__main__":
    main()

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
import sys
import argparse

# Enable pandas copy-on-write mode for memory optimization
pd.options.mode.copy_on_write = True


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


async def async_insert_messages_to_postgres(messages, db_config):
    """
    Asynchronously inserts a batch of messages into the PostgreSQL database.
    Deduplicates messages based on message_id before insertion.

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

    # Deduplicate messages based on message_id
    unique_messages = {}
    for message in messages:
        unique_messages[message["message_id"]] = message

    deduplicated_messages = list(unique_messages.values())

    if len(deduplicated_messages) < len(messages):
        print(
            f"Deduplicated {len(messages) - len(deduplicated_messages)} messages with duplicate message_ids"
        )

    try:
        execute_values(
            cursor,
            f"""
            INSERT INTO {table_name} (message_id, timestamp_usec, timestamp_text, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text)
            VALUES %s
            ON CONFLICT (message_id) DO UPDATE SET
                timestamp_usec = EXCLUDED.timestamp_usec,
                timestamp_text = EXCLUDED.timestamp_text,
                video_id = EXCLUDED.video_id,
                author = EXCLUDED.author,
                author_channel_id = EXCLUDED.author_channel_id,
                message = EXCLUDED.message,
                is_moderator = EXCLUDED.is_moderator,
                is_channel_owner = EXCLUDED.is_channel_owner,
                video_offset_time_msec = EXCLUDED.video_offset_time_msec,
                video_offset_time_text = EXCLUDED.video_offset_time_text
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
                for message in deduplicated_messages
            ],
        )
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        # Print a sample of the problematic rows to help with debugging
        if deduplicated_messages:
            print(f"Sample of rows being inserted (first 3):")
            for i, msg in enumerate(deduplicated_messages[:3]):
                print(f"Row {i+1}: {msg}")
        raise

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
                replay_chat_item_action = obj.get("replayChatItemAction", {})
                if not replay_chat_item_action:
                    print(f"Skipping line without replayChatItemAction in {json_path}")
                    continue
                actions = replay_chat_item_action.get("actions", [])
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
                            "videoOffsetTimeMsec",
                            obj.get("replayChatItemAction", {}).get(
                                "videoOffsetTimeMsec", None
                            ),
                        )

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
            except Exception as e:
                print(f"Error processing line in {json_path}: {e}")
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


async def process_files_to_postgres(file_paths, db_config, buffer_size=10000):
    """
    Asynchronously processes multiple JSON files, buffering messages and inserting them into PostgreSQL.
    This is an alias for process_files_to_postgres_async for backward compatibility.

    Parameters:
        file_paths (list): List of file paths to process.
        db_config (dict): Database configuration for PostgreSQL connection.
        buffer_size (int): Number of messages to buffer before inserting.
    """
    await process_files_to_postgres_async(file_paths, db_config, buffer_size)


def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


def search_messages(db_config, regex_pattern, window_size=60, min_matches=5):
    """
    Searches the PostgreSQL database for messages matching a regex pattern and finds windows of `window_size` seconds starting with the matching text.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.

    Returns:
        list: A list of dictionaries containing grouped search results.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Query to fetch messages and metadata
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
    cursor.execute(query)
    rows = cursor.fetchall()

    # Convert rows to a pandas DataFrame
    df = pd.DataFrame(
        rows,
        columns=[
            "timestamp_usec",
            "video_id",
            "video_offset_time_msec",
            "message",
            "release_timestamp",
            "title",
        ],
    )

    # Ensure video_offset_time_msec is an integer
    df["video_offset_time_msec"] = df["video_offset_time_msec"].fillna(0).astype(int)

    # Convert timestamp_usec to datetime for easier processing
    df["timestamp"] = pd.to_datetime(df["timestamp_usec"], unit="us")

    # Filter rows matching the regex pattern
    pattern = re.compile(regex_pattern)
    df["matches"] = df["message"].apply(lambda x: bool(pattern.search(x)))
    matching_df = df[df["matches"]]

    # Find windows of `window_size` seconds starting with the matching text
    results = []
    for _, match_row in matching_df.iterrows():
        start_time = match_row["timestamp"]
        end_time = start_time + pd.Timedelta(seconds=window_size)

        # Filter for matches within the window and from the same video
        window_df = matching_df[
            (matching_df["timestamp"] >= start_time)
            & (matching_df["timestamp"] < end_time)
            & (matching_df["video_id"] == match_row["video_id"])
        ]

        # Exclude consecutive matches from the same video that are less than `window_size` seconds apart
        if not results or (
            results[-1]["video_id"] != match_row["video_id"]
            or (
                match_row["timestamp"]
                - pd.to_datetime(results[-1]["timestamp_usec"], unit="us")
            ).total_seconds()
            >= window_size
        ):
            if len(window_df) >= min_matches:
                first_message = window_df.iloc[0]
                results.append(
                    {
                        "video_id": first_message["video_id"],
                        "video_date": pd.to_datetime(
                            first_message["release_timestamp"]
                        ).strftime("%Y-%m-%d"),
                        "video_title": first_message["title"],
                        "video_offset_time_seconds": first_message[
                            "video_offset_time_msec"
                        ]
                        // 1000,
                        "timestamp_usec": first_message["timestamp_usec"],
                        "message": first_message["message"],
                    }
                )

    conn.close()
    # Ensure results are sorted by timestamp_usec (oldest to newest) before returning
    return sorted(results, key=lambda x: x["timestamp_usec"])


def print_search_results_as_markdown(
    db_config, regex_pattern, window_size=60, min_matches=5, timestamp_offset=10
):
    """
    Searches the database and prints results as a markdown table with columns:
    - Video Date (YYYY-mm-dd)
    - Video Title (as a YouTube link)
    - Timestamp Link (HH:MM:SS)
    - Message Text

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.
        regex_pattern (str): Regex pattern to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
        timestamp_offset (int): Number of seconds to subtract from the timestamp for context.
    """
    results = search_messages(db_config, regex_pattern, window_size, min_matches)

    # Print results as a markdown table
    print(f"Search pattern: `{regex_pattern}`")
    print("| Date | Title | Timestamp")
    print("|-------|-------|----------|")
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
            f"| {result['video_date']} | [{result['video_title']}]({video_link}) | [{timestamp_hms}]({timestamp_link}) |"
        )


def parse_duration(duration_string):
    """
    Converts a duration string to seconds.

    Parameters:
        duration_string (str): The duration string in HH:MM:SS, MM:SS, or SS format.

    Returns:
        int or None: Duration in seconds, or None if the string is invalid.
    """
    if duration_string:
        duration_parts = list(map(int, duration_string.split(":")))
        if len(duration_parts) == 3:
            return duration_parts[0] * 3600 + duration_parts[1] * 60 + duration_parts[2]
        elif len(duration_parts) == 2:
            return duration_parts[0] * 60 + duration_parts[1]
        elif len(duration_parts) == 1:
            return duration_parts[0]
    return None


def parse_info_json_to_sqlite(json_path, db_path="chat_messages.db"):
    """
    Parses a YouTube video info JSON file and inserts metadata into a SQLite database.
    Creates a table called "video_metadata" with columns: video_id, title, channel_id, channel_name, release_timestamp, duration_seconds.
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
            release_timestamp TEXT,
            duration_seconds INTEGER
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
            duration = data.get("duration", None)
            if not duration:
                duration_string = data.get("duration_string", None)
                duration = parse_duration(duration_string)

            # Convert timestamp to ISO 8601 format with time (YYYY-MM-DD HH:MM:SS.SSSZ)
            release_timestamp = ""
            if timestamp:
                release_timestamp = f"{pd.to_datetime(timestamp, unit='s').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}Z"

            # Insert metadata into the database
            cursor.execute(
                """
                INSERT OR IGNORE INTO video_metadata (video_id, title, channel_id, channel_name, release_timestamp, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    video_id,
                    title,
                    channel_id,
                    channel_name,
                    release_timestamp,
                    duration,
                ),
            )
    except Exception as e:
        print(f"Error processing file {json_path}: {e}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()


async def parse_info_json_to_postgres(json_path, db_config):
    """
    Parses a YouTube video info JSON file and inserts metadata into a PostgreSQL database.
    Creates a table called "video_metadata" with columns: video_id, title, channel_id, channel_name, release_timestamp, duration_seconds, was_live.
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
            release_timestamp TIMESTAMP,
            duration_seconds INTEGER,
            was_live BOOLEAN
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
            duration = data.get("duration", None)
            if not duration:
                duration_string = data.get("duration_string", None)
                duration = parse_duration(duration_string)

            was_live = data.get("was_live", None)

            # Convert timestamp to native PostgreSQL TIMESTAMP format in UTC
            release_timestamp = None
            if timestamp:
                release_timestamp = pd.to_datetime(timestamp, unit="s", utc=True)

            # Insert metadata into the database
            cursor.execute(
                """
                INSERT INTO video_metadata (video_id, title, channel_id, channel_name, release_timestamp, duration_seconds, was_live)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    channel_id = EXCLUDED.channel_id,
                    channel_name = EXCLUDED.channel_name,
                    release_timestamp = EXCLUDED.release_timestamp,
                    duration_seconds = EXCLUDED.duration_seconds,
                    was_live = EXCLUDED.was_live
                """,
                (
                    video_id,
                    title,
                    channel_id,
                    channel_name,
                    release_timestamp,
                    duration,
                    was_live,
                ),
            )
    except Exception as e:
        print(f"Error processing file {json_path}: {e}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()


async def parse_jsons_to_postgres(directory_path, db_config, json_type="live_chat"):
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
    elif json_type == "info":
        file_pattern = "**/*.info.json"
    else:
        print(f"Error: Unsupported JSON type '{json_type}'.")
        return

    # Find all matching JSON files in the directory tree
    json_files = glob.glob(f"{escaped_path}/{file_pattern}", recursive=True)

    if json_type == "live_chat":
        await process_files_to_postgres_async(json_files, db_config)
    else:
        # Process info JSON files asynchronously
        for json_file in json_files:
            await parse_info_json_to_postgres(json_file, db_config)

    print(
        f"Processed {len(json_files)} {json_type} files into the PostgreSQL database."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Process YouTube data. Allows parsing JSON files or searching messages."
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="actions",
        description="Choose an action to perform:",
        help="Run '<command> --help' for more information on a specific command.",
    )

    # Search sub-command
    search_parser = subparsers.add_parser(
        "search", help="Search messages and print results as markdown."
    )
    search_parser.add_argument(
        "regex_pattern",
        metavar="REGEX_PATTERN",
        type=str,
        help="Regex pattern to search for in messages.",
    )

    # Parse sub-command
    parse_parser = subparsers.add_parser(
        "parse", help="Parse JSON files and load into PostgreSQL."
    )
    parse_parser.add_argument(
        "--info-json",
        metavar="DIRECTORY_PATH",
        type=str,
        help="Directory path containing info JSON files to parse.",
    )
    parse_parser.add_argument(
        "--live-chat-json",
        metavar="DIRECTORY_PATH",
        type=str,
        help="Directory path containing live chat JSON files to parse.",
    )

    args = parser.parse_args()

    db_config = {
        "dbname": "ytlc",
        "user": "ytlc",
        "host": "localhost",
        "port": 5432,
    }

    if args.command == "search":
        print_search_results_as_markdown(db_config, args.regex_pattern)

    elif args.command == "parse":
        if not args.info_json and not args.live_chat_json:
            parse_parser.error(
                "For the 'parse' command, you must specify --info-json and/or --live-chat-json path(s)."
            )

        if args.info_json:
            directory_path_info = args.info_json
            if not os.path.isdir(directory_path_info):
                parse_parser.error(
                    f"Directory for --info not found at {directory_path_info}"
                )
            print(f"Parsing info JSON files from: {directory_path_info}")
            asyncio.run(
                parse_jsons_to_postgres(
                    directory_path_info, db_config, json_type="info"
                )
            )

        if args.live_chat_json:
            directory_path_live_chat_json = args.live_chat_json
            if not os.path.isdir(directory_path_live_chat_json):
                parse_parser.error(
                    f"Directory for --live-chat not found at {directory_path_live_chat_json}"
                )
            print(f"Parsing live chat JSON files from: {directory_path_live_chat_json}")
            asyncio.run(
                parse_jsons_to_postgres(
                    directory_path_live_chat_json, db_config, json_type="live_chat"
                )
            )


if __name__ == "__main__":
    main()

import os
import glob
import orjson
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import asyncio
import re


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


async def async_parse_live_chat_json_buffered(json_path, buffer_size=10000):
    """
    Asynchronously parses a YouTube live chat JSONL file and yields messages in buffered batches.

    Parameters:
        json_path (str): Path to the JSONL file.
        buffer_size (int): Number of messages to buffer before yielding.

    Yields:
        list: A list of buffered messages.
    """
    canonicalized_path = os.path.realpath(json_path)
    buffer = []
    video_id = extract_video_id_from_filename(canonicalized_path)

    with open(canonicalized_path, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                obj = orjson.loads(line)
                replay_chat_item_action = obj.get("replayChatItemAction", {})
                if not replay_chat_item_action:
                    print(f"Skipping line without replayChatItemAction in {canonicalized_path}")
                    continue
                actions = replay_chat_item_action.get("actions", [])
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    renderer = item.get("liveChatTextMessageRenderer")
                    if renderer:
                        # Extract message text (concatenate all runs)
                        runs = renderer.get("message", {}).get("runs", [])
                        msg = parse_message_runs(runs)
                        timestamp = pd.to_datetime(
                            int(renderer.get("timestampUsec", 0)),
                            unit="us",
                            utc=True,
                            origin="unix",
                        )
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
                                "timestamp": timestamp,
                                "video_id": video_id,
                                "author": author,
                                "author_channel_id": author_channel_id,
                                "message": msg,
                                "is_moderator": is_moderator,
                                "is_channel_owner": is_channel_owner,
                                "video_offset_time_msec": video_offset_time_msec,
                                "video_offset_time_text": video_offset_time_text,
                                "canonicalized_path": canonicalized_path,
                            }
                        )

                        if len(buffer) >= buffer_size:
                            yield buffer
                            buffer = []
            except Exception as e:
                print(f"Error processing line in {canonicalized_path}: {e}")
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


async def parse_info_json_to_postgres(json_path, db_config):
    """
    Parses a YouTube video info JSON file and inserts metadata into a PostgreSQL database.
    Creates a table called "video_metadata" with columns: video_id, title, channel_id, channel_name, release_timestamp, duration, was_live, filename.
    """
    canonicalized_path = os.path.realpath(json_path)

    # Connect to PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'UTC';")

    # Create the video_metadata table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS video_metadata (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            channel_id TEXT,
            channel_name TEXT,
            release_timestamp TIMESTAMPTZ,
            timestamp TIMESTAMPTZ,
            duration INTERVAL,
            was_live BOOLEAN,
            filename TEXT
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
            release_timestamp = pd.to_datetime(
                int(data["release_timestamp"]) if "release_timestamp" in data else None,
                unit="s",
                utc=True,
                origin="unix",
            )
            timestamp = pd.to_datetime(
                int(data["timestamp"]) if "timestamp" in data else None,
                unit="s",
                utc=True,
                origin="unix",
            )
            duration = data.get("duration", None)
            if not duration:
                duration_string = data.get("duration_string", None)
                duration = parse_duration(duration_string)

            was_live = data.get("was_live", None)

            # Insert metadata into the database
            cursor.execute(
                """
                INSERT INTO video_metadata (video_id, title, channel_id, channel_name, release_timestamp, timestamp, duration, was_live, filename)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    title = COALESCE(NULLIF(EXCLUDED.title, ''), video_metadata.title),
                    channel_id = COALESCE(NULLIF(EXCLUDED.channel_id, ''), video_metadata.channel_id),
                    channel_name = COALESCE(NULLIF(EXCLUDED.channel_name, ''), video_metadata.channel_name),
                    release_timestamp = COALESCE(EXCLUDED.release_timestamp, video_metadata.release_timestamp),
                    timestamp = COALESCE(EXCLUDED.timestamp, video_metadata.timestamp),
                    duration = COALESCE(EXCLUDED.duration, video_metadata.duration),
                    was_live = COALESCE(EXCLUDED.was_live, video_metadata.was_live),
                    filename = COALESCE(EXCLUDED.filename, video_metadata.filename)
                """,
                (
                    video_id,
                    title,
                    channel_id,
                    channel_name,
                    release_timestamp,
                    timestamp,
                    pd.Timedelta(seconds=duration) if duration else None,
                    was_live,
                    canonicalized_path,
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


def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


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
    cursor.execute("SET TIME ZONE 'UTC';")
    table_name = "live_chat"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            message_id TEXT PRIMARY KEY,
            timestamp TIMESTAMPTZ,
            video_id TEXT,
            author TEXT,
            author_channel_id TEXT,
            message TEXT,
            is_moderator BOOLEAN,
            is_channel_owner BOOLEAN,
            video_offset_time_msec BIGINT,
            video_offset_time_text TEXT,
            filename TEXT
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
            INSERT INTO {table_name} (message_id, timestamp, video_id, author, author_channel_id, message, is_moderator, is_channel_owner, video_offset_time_msec, video_offset_time_text, filename)
            VALUES %s
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
            """,
            [
                (
                    message["message_id"],
                    message["timestamp"],
                    message["video_id"],
                    message["author"],
                    message["author_channel_id"],
                    message["message"],
                    message["is_moderator"],
                    message["is_channel_owner"],
                    message["video_offset_time_msec"],
                    message["video_offset_time_text"],
                    message["canonicalized_path"],
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

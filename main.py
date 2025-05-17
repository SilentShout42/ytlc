import json
import csv
import os
import pandas as pd
import sqlite3
import glob
import re


def parse_offset(ts):
    """
    Converts a timestamp string (e.g., "1:23:45" or "-0:42") to signed seconds offset.
    """
    sign = -1 if str(ts).startswith("-") else 1
    ts_clean = str(ts).lstrip("+-")
    parts = ts_clean.split(":")
    parts = [int(p) for p in parts]
    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h = 0
        m, s = parts
    elif len(parts) == 1:
        h = 0
        m = 0
        s = parts[0]
    else:
        return 0
    total = sign * (h * 3600 + m * 60 + s)
    return total


def csv_to_histogram(csv_path):
    # Load your data
    df = pd.read_csv(csv_path)

    # Convert timestamp_text to signed seconds offset
    df["offset"] = df["timestamp_text"].apply(parse_offset)

    # Set offset as index
    df.set_index("offset", inplace=True)

    # Resample to 1 minute bins (60s), filling missing with 0
    # Group by offset//60 to get per-minute bins
    df["minute"] = df.index // 60
    message_counts = df.groupby("minute").size()

    # Output: minute offset, count
    message_counts.to_csv("histogram_data.dat", header=False)


def parse_live_chat_json_to_csv(json_path):
    """
    Parses a YouTube live chat JSONL file and writes messages to a CSV.
    CSV columns: timestamp_usec, timestamp_text, author, message, is_moderator, is_channel_owner
    """
    # Replace .live_chat.json with .csv in the same directory
    if json_path.endswith(".live_chat.json"):
        csv_path = json_path.replace(".live_chat.json", ".csv")
    else:
        csv_path = json_path + ".csv"

    rows = []

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
                        timestamp_usec = renderer.get("timestampUsec", "")
                        timestamp_text = renderer.get("timestampText", {}).get(
                            "simpleText", ""
                        )
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        # Check for moderator badge
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
                        rows.append(
                            [
                                timestamp_usec,
                                timestamp_text,
                                author,
                                msg,
                                is_moderator,
                                is_channel_owner,
                            ]
                        )
            except Exception as e:
                continue

    # Sort rows by timestamp_usec before writing to CSV
    rows.sort(key=lambda x: int(x[0]))

    with open(csv_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(
            [
                "timestamp_usec",
                "timestamp_text",
                "author",
                "message",
                "is_moderator",
                "is_channel_owner",
            ]
        )
        writer.writerows(rows)

    return csv_path


def parse_live_chat_json_to_sqlite(json_path, db_path="chat_messages.db"):
    """
    Parses a YouTube live chat JSONL file and inserts messages into a SQLite database.
    Each video will have its own table named after its video ID.
    Ensures duplicates are not inserted when re-processing a file.
    """
    # Extract video ID from the JSON filename
    video_id = extract_video_id_from_filename(json_path)

    # Sanitize the table name by replacing invalid characters with underscores
    sanitized_video_id = re.sub(r"[^A-Za-z0-9_]", "_", video_id)
    table_name = f"video_{sanitized_video_id}"

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    conn.text_factory = str  # Ensure support for international characters
    cursor = conn.cursor()

    # Create a table for the video if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp_usec INTEGER PRIMARY KEY,
            timestamp_text TEXT,
            author TEXT,
            message TEXT,
            is_moderator BOOLEAN,
            is_channel_owner BOOLEAN
        )
    """)

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
                        timestamp_text = renderer.get("timestampText", {}).get("simpleText", "")
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        # Check for moderator and channel owner badges
                        is_moderator = False
                        is_channel_owner = False
                        for badge in renderer.get("authorBadges", []):
                            badge_renderer = badge.get("liveChatAuthorBadgeRenderer", {})
                            icon_type = badge_renderer.get("icon", {}).get("iconType", "")
                            if icon_type == "MODERATOR":
                                is_moderator = True
                            elif icon_type == "OWNER":
                                is_channel_owner = True

                        # Insert the message into the database, escaping strings to prevent SQL injection
                        cursor.execute(f"""
                            INSERT OR IGNORE INTO {table_name} (timestamp_usec, timestamp_text, author, message, is_moderator, is_channel_owner)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (timestamp_usec, timestamp_text, author, msg, is_moderator, is_channel_owner))
            except Exception as e:
                continue

    # Commit changes and close the connection
    conn.commit()
    conn.close()


def parse_live_chat_jsons_to_sqlite(directory_path, db_path="chat_messages.db"):
    """
    Parses all YouTube live chat JSONL files in a directory tree and inserts messages into a SQLite database.
    Each video will have its own table named after its video ID.
    Ensures duplicates are not inserted when re-processing files.
    """
    # Ensure the directory exists
    if not os.path.exists(directory_path):
        print(f"Error: Directory '{directory_path}' does not exist.")
        return

    # Escape the directory path for glob
    escaped_path = glob.escape(directory_path)

    # Find all .live_chat.json files in the directory tree
    json_files = glob.glob(f"{escaped_path}/**/*.live_chat.json", recursive=True)

    for json_file in json_files:
        parse_live_chat_json_to_sqlite(json_file, db_path)

    print(f"Processed {len(json_files)} files into the SQLite database: {db_path}")


def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    match = re.search(r"\[([A-Za-z0-9_-]{11})\]", filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)


def output_top10_links(csv_path):
    """
    Identifies the top 10 busiest 1-minute intervals in a CSV file and generates YouTube timestamp links.
    """
    # Get video ID from filename
    base = os.path.basename(csv_path)
    try:
        video_id = extract_video_id_from_filename(base)
    except ValueError as e:
        print(f"Error extracting video ID: {e}")
        return

    # Load CSV and compute per-minute message counts
    df = pd.read_csv(csv_path)

    # Convert timestamp_text to signed seconds offset
    df["offset"] = df["timestamp_text"].apply(parse_offset)
    df["minute"] = df["offset"] // 60
    message_counts = df.groupby("minute").size()

    # Get top 10 minute intervals
    top10_by_count = message_counts.sort_values(ascending=False).head(10)

    # Generate YouTube timestamp links
    for minute, count in top10_by_count.items():
        timestamp = f"{minute // 60}:{minute % 60:02d}"
        link = f"https://www.youtube.com/watch?v={video_id}&t={minute * 60}s"
        print(f"{timestamp} ({count} messages): {link}")


def main():
    directory_path = r"/home/localuser/mnt/media/youtube/out/Kanna_Yanagi_ch._[UClxj3GlGphZVgd1SLYhZKmg]"
    # db_path = parse_live_chat_jsons_to_sqlite(directory_path)


if __name__ == "__main__":
    main()

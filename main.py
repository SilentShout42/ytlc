import json
import csv
import os
import pandas as pd

def parse_offset(ts):
    """
    Converts a timestamp string (e.g., "1:23:45" or "-0:42") to signed seconds offset.
    """
    sign = -1 if str(ts).startswith('-') else 1
    ts_clean = str(ts).lstrip('+-')
    parts = ts_clean.split(':')
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
    df['offset'] = df['timestamp_text'].apply(parse_offset)

    # Set offset as index
    df.set_index('offset', inplace=True)

    # Resample to 1 minute bins (60s), filling missing with 0
    # Group by offset//60 to get per-minute bins
    df['minute'] = (df.index // 60)
    message_counts = df.groupby('minute').size()

    # Output: minute offset, count
    message_counts.to_csv('histogram_data.dat', header=False)

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
                        timestamp_text = renderer.get("timestampText", {}).get("simpleText", "")
                        author = renderer.get("authorName", {}).get("simpleText", "")
                        # Check for moderator badge
                        is_moderator = False
                        is_channel_owner = False
                        for badge in renderer.get("authorBadges", []):
                            badge_renderer = badge.get("liveChatAuthorBadgeRenderer", {})
                            icon_type = badge_renderer.get("icon", {}).get("iconType", "")
                            if icon_type == "MODERATOR":
                                is_moderator = True
                            elif icon_type == "OWNER":
                                is_channel_owner = True
                        rows.append([timestamp_usec, timestamp_text, author, msg, is_moderator, is_channel_owner])
            except Exception as e:
                continue

    # Sort rows by timestamp_usec before writing to CSV
    rows.sort(key=lambda x: int(x[0]))

    with open(csv_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["timestamp_usec", "timestamp_text", "author", "message", "is_moderator", "is_channel_owner"])
        writer.writerows(rows)

    return csv_path

def extract_video_id_from_filename(filename):
    """
    Extracts the video ID from a filename enclosed in square brackets.
    """
    import re
    match = re.search(r'\[([A-Za-z0-9_-]{11})\]', filename)
    if not match:
        raise ValueError(f"Could not extract video ID from filename: {filename}")
    return match.group(1)

def output_top10_links(csv_path):
    # Get video ID from filename
    base = os.path.basename(csv_path)
    try:
        video_id = extract_video_id_from_filename(base)
    except ValueError as e:
        print(e)
        return

    # Load CSV and compute per-minute message counts
    df = pd.read_csv(csv_path)

    # Convert timestamp_text to signed seconds offset
    df['offset'] = df['timestamp_text'].apply(parse_offset)
    df['minute'] = (df['offset'] // 60)
    message_counts = df.groupby('minute').size()

    # Get top 10 minute intervals
    top10_by_count = message_counts.sort_values(ascending=False).head(10)

    # Sort these top 10 intervals by minute (timestamp order)
    top10_sorted_by_time = top10_by_count.sort_index()

    # Output links
    print("\nTop 10 busiest 1-minute intervals (sorted by time):")
    for minute, count in top10_sorted_by_time.items():
        t = int(minute * 60) - 90
        # Ensure timestamp is not negative for the URL
        t_for_url = max(0, t)
        print(f"https://www.youtube.com/watch?v={video_id}&t={t_for_url}s ({count} messages)")

def main():
    json_file = r"/home/localuser/mnt/media/youtube/out/Kanna_Yanagi_ch._[UClxj3GlGphZVgd1SLYhZKmg]/2025/05/2025-05-15--15-55_Let_s_The_Journey_Start_First_Doom_Game_DOOM_-_The_Dark_Ages_Blind_Playthrough_[CzU927ARnkA].live_chat.json"
    csv_file = parse_live_chat_json_to_csv(json_file)
    output_top10_links(csv_file)

if __name__ == "__main__":
    main()

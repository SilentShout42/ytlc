import json
import csv
import os

def parse_live_chat_json_to_csv(json_path):
    """
    Parses a YouTube live chat JSONL file and writes messages to a CSV.
    CSV columns: timestamp_usec, timestamp_text, author, message, is_moderator
    """
    # Replace .live_chat.json with .csv in the same directory
    if json_path.endswith(".live_chat.json"):
        csv_path = json_path.replace(".live_chat.json", ".csv")
    else:
        csv_path = json_path + ".csv"

    with open(json_path, "r", encoding="utf-8") as infile, \
         open(csv_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["timestamp_usec", "timestamp_text", "author", "message", "is_moderator"])

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
                        for badge in renderer.get("authorBadges", []):
                            badge_renderer = badge.get("liveChatAuthorBadgeRenderer", {})
                            if badge_renderer.get("icon", {}).get("iconType", "") == "MODERATOR":
                                is_moderator = True
                        writer.writerow([timestamp_usec, timestamp_text, author, msg, is_moderator])
            except Exception as e:
                continue

def main():
    parse_live_chat_json_to_csv(
        "samples/2025-05-13--16-00_Duck_Detective_Solving_Mysteries_I_m_On_The_Case~_Blue_Prince_[70Ew-NPBGG4].live_chat.json"
    )

if __name__ == "__main__":
    main()

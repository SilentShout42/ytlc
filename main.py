import os
import re
import sqlite3
import argparse
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict
from math import ceil
from rich.markdown import Markdown
from rich.markup import escape
from rich import print

from parser import parse_jsons_to_sqlite


def sqlite_query(db_path: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SQL query and return results as a list of dicts."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def search_messages(
    db_path: str,
    regex_patterns: list[str],
    window_size: int = 60,
    min_matches: int = 5,
) -> tuple[list[dict], str | None, int]:
    """
    Search the SQLite database for messages matching regex patterns.
    Uses LIKE for initial pruning, then in-memory regex filter, then
    windowing (≥ min_matches per window_size per video).

    Returns: (results, latest_timestamp_str, total_lines)
    """
    like_params: list[str] = [f"%{p}%" for p in regex_patterns]
    like_placeholders = " OR ".join(
        f"lc.message LIKE ?{i + 1}" for i in range(len(like_params))
    )

    fetch_query = f"""
        SELECT
            lc.timestamp,
            lc.video_id,
            CAST(ROUND(
                (julianday(lc.timestamp) - julianday(vm.release_timestamp)) * 86400
            ) AS INTEGER) AS video_offset_time_seconds,
            lc.message,
            lc.author,
            vm.title,
            lc.video_offset_time_msec
        FROM live_chat lc
        JOIN video_metadata vm ON lc.video_id = vm.video_id
    """

    # Compile patterns for in-memory filtering
    match_patterns = [re.compile(f"(?i){p}") for p in regex_patterns]

    data: list[dict] = []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # LIKE filter is a fast pre-filter; LIKE can't handle full regex
        # syntax (e.g. "((bless (yo)?u)\|gesund" is literal for LIKE), so
        # the result set is empty when the pattern is pure regex — we then
        # fall back to the full scan below.
        filtered_query = f"{fetch_query} WHERE {like_placeholders} ORDER BY lc.video_id, lc.timestamp"
        rows = conn.execute(filtered_query, like_params).fetchall()
        data = [dict(r) for r in rows]
        data = [r for r in data if any(p.search(r["message"]) for p in match_patterns)]
    except Exception:
        pass

    # Fall back to full scan + in-memory regex
    if not data:
        rows = conn.execute(fetch_query).fetchall()
        data = [dict(r) for r in rows]
        data = [r for r in data if any(p.search(r["message"]) for p in match_patterns)]

    total_rows = conn.execute("SELECT COUNT(*) FROM live_chat").fetchone()[0]
    latest_row = conn.execute("SELECT MAX(timestamp) FROM live_chat").fetchone()
    latest_str = latest_row[0] if latest_row[0] else None
    conn.close()

    # Windowing: group by video_id, find windows with >= min_matches
    groups: list[tuple[str, list[dict]]] = []
    seen: set[str] = set()
    for r in data:
        if r["video_id"] not in seen:
            seen.add(r["video_id"])
            groups.append((r["video_id"], []))
        groups[-1][1].append(r)

    results: list[dict] = []
    for video_id, group_rows in groups:
        group_rows.sort(key=lambda r: r["timestamp"])
        for i in range(len(group_rows)):
            start_ts = datetime.fromisoformat(group_rows[i]["timestamp"])
            end_ts = start_ts + timedelta(seconds=window_size)
            window_rows = [
                r for r in group_rows[i:]
                if datetime.fromisoformat(r["timestamp"]) >= start_ts
                and datetime.fromisoformat(r["timestamp"]) < end_ts
            ]
            if len(window_rows) >= min_matches:
                last = results[-1] if results else None
                already = (
                    last is not None
                    and last["video_id"] == video_id
                    and (start_ts - datetime.fromisoformat(last["timestamp"])).total_seconds()
                    < window_size
                ) if last else False
                if not already:
                    results.append(group_rows[i])

    results.sort(key=lambda r: r["timestamp"])
    return results, latest_str, total_rows


def count_missing_video_days(db_path: str) -> tuple[int, list]:
    """Count days with no videos since 2024-05-25."""
    effective_start = date(2024, 5, 25)
    today = date.today()
    end_of_period = today - timedelta(days=1)

    if end_of_period < effective_start:
        return 0, []

    query = """
        SELECT DISTINCT strftime('%Y-%m-%d', release_timestamp) as video_date
        FROM video_metadata
        WHERE release_timestamp >= ? AND release_timestamp < ?
    """
    db_dates = set(
        r["video_date"]
        for r in sqlite_query(
            db_path, query,
            (effective_start.isoformat(), today.isoformat()),
        )
    )

    all_dates = [effective_start + timedelta(days=i) for i in range((end_of_period - effective_start).days + 1)]
    missing_dates = sorted([d for d in all_dates if d.isoformat() not in db_dates])
    return len(missing_dates), missing_dates


def get_video_offsets(db_path: str) -> dict:
    """Build video_id -> offset_seconds mapping from minimum timestamp diff."""
    rows = sqlite_query(
        db_path,
        """
        SELECT vm.video_id,
               CAST(ROUND((julianday(lc.timestamp) - julianday(vm.release_timestamp)) * 86400) AS INTEGER) AS offset_seconds
        FROM live_chat lc
        JOIN video_metadata vm ON lc.video_id = vm.video_id
        WHERE lc.timestamp >= vm.release_timestamp
        GROUP BY vm.video_id
        """,
    )
    return {r["video_id"]: int(r["offset_seconds"]) for r in rows}


def print_search_results_as_markdown(
    db_path: str,
    regex_patterns: list[str],
    window_size: int = 60,
    min_matches: int = 5,
    timestamp_offset: int = -10,
    output_file: str | None = None,
    debug: bool = False,
) -> None:
    """Search and print results as a markdown table."""
    results, latest_str, total_lines = search_messages(
        db_path, regex_patterns, window_size, min_matches
    )

    headers = ["Date", "Title", "Timestamp"]
    if debug:
        headers.extend(["Author", "Message"])

    header_line = f"| {' | '.join(headers)} |"
    spacer_line = f"|{'------|' * len(headers)}"
    output_lines = [header_line, spacer_line]

    # Group hits by video_id, preserving first-occurrence order
    groups: list[tuple[str, list[dict]]] = []
    seen: set[str] = set()
    for r in results:
        if r["video_id"] not in seen:
            seen.add(r["video_id"])
            groups.append((r["video_id"], [r]))
        else:
            groups[-1][1].append(r)

    for video_id, rows in groups:
        first = rows[0]
        video_link = f"https://www.youtube.com/watch?v={video_id}"

        # Format timestamp links for all matches in this group
        timestamp_links: list[str] = []
        for r in rows:
            msec = r.get("video_offset_time_msec", 0)
            if msec and msec > 0:
                offset_secs = msec / 1000
            else:
                offset_secs = r.get("video_offset_time_seconds", 0) or 0

            adjusted = int(ceil(offset_secs + timestamp_offset))
            adjusted = max(0, adjusted)
            h, remainder = divmod(adjusted, 3600)
            m, s = divmod(remainder, 60)
            timestamp_links.append(
                f"[{h:02d}:{m:02d}:{s:02d}]({video_link}&t={adjusted}s)"
            )

        row = [
            first["timestamp"][:10],  # "YYYY-MM-DD" from stored format
            f"[{first['title']}]({video_link})",
            ", ".join(timestamp_links),
        ]

        if debug:
            row.extend([first.get("author", ""), first.get("message", "")])

        output_lines.append(f"| {' | '.join(row)} |")

    # Summary table
    output_lines.append("")
    output_lines.append("| Parameter       | Value |")
    output_lines.append("|-----------------|-------|")

    escaped_patterns = [
        re.sub(r"([*_~|`])", r"\\\\\1", p) for p in regex_patterns
    ]
    output_lines.append(
        f"| Search Patterns | `{', '.join(escaped_patterns)}` |"
    )
    output_lines.append(f"| Window Size     | {window_size} seconds |")
    output_lines.append(f"| Minimum Matches | {min_matches} |")
    output_lines.append(f"| Results Found   | {len(results)} |")
    output_lines.append(f"| Lines Searched  | {total_lines} |")
    output_lines.append(
        f"| Generated At    | {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} |"
    )

    if latest_str:
        dt = datetime.fromisoformat(latest_str)
        output_lines.append(
            f"| Latest Live Chat | {dt.strftime('%Y-%m-%d %H:%M:%S UTC')} |"
        )

    escaped_output = "\n".join(escape(line) for line in output_lines)
    print(Markdown(escaped_output))

    if output_file:
        with open(output_file, "w") as f:
            f.write(escaped_output)


def dbcheck(db_path: str) -> None:
    """Test database connection and show basic stats."""
    print(f"Opening SQLite database at {db_path}...")
    conn = sqlite3.connect(db_path)

    version = conn.execute("SELECT sqlite_version()").fetchone()[0]
    print(f"OK — SQLite {version}")

    for table in ["live_chat", "video_metadata"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except Exception as e:
            print(f"  {table}: error — {e}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Process YouTube live chat data. Parse JSON files or search messages."
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="actions",
        description="Choose an action to perform:",
        help="Run '<command> --help' for more info on a command.",
    )

    # Search sub-command
    search_parser = subparsers.add_parser(
        "search", help="Search messages and print results as markdown."
    )
    search_parser.add_argument(
        "regex_patterns",
        metavar="REGEX_PATTERNS",
        type=str,
        nargs="+",
        help="List of regex patterns to search for in messages.",
    )
    search_parser.add_argument(
        "-o", "--output-file", metavar="OUTPUT_FILE", type=str,
        help="File to write search results to.",
    )
    search_parser.add_argument(
        "--debug", action="store_true",
        help="Include Author and Message columns in the output.",
    )

    # Parse sub-command
    parse_parser = subparsers.add_parser(
        "parse", help="Parse JSON files and load into SQLite."
    )
    parse_parser.add_argument(
        "data_dir",
        metavar="DATA_DIR",
        type=str,
        help="Directory containing .info.json and .live_chat.json files.",
    )

    # DB check sub-command
    subparsers.add_parser("dbcheck", help="Test database connection and show stats.")

    args = parser.parse_args()

    # Match Rust behavior: read from YTLC_DB env var, default to ytlc.db
    db_path = os.environ.get("YTLC_DB", "ytlc.db")

    if args.command == "search":
        print_search_results_as_markdown(
            db_path,
            args.regex_patterns,
            window_size=60,
            min_matches=5,
            timestamp_offset=-10,
            output_file=args.output_file,
            debug=args.debug,
        )

    elif args.command == "parse":
        if not os.path.isdir(args.data_dir):
            parse_parser.error(f"Directory not found at {args.data_dir}")

        print(f"Parsing JSON files from: {args.data_dir}")
        parse_jsons_to_sqlite(args.data_dir, db_path, "info")
        parse_jsons_to_sqlite(args.data_dir, db_path, "live_chat")

    elif args.command == "dbcheck":
        dbcheck(db_path)


if __name__ == "__main__":
    main()

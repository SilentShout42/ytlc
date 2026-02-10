import json
import os
import pandas as pd
import re
import asyncio
import argparse
from rich.markdown import Markdown
from rich.console import Console
from rich.markup import escape
from rich import print
from math import ceil
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Enable pandas copy-on-write mode for memory optimization
pd.options.mode.copy_on_write = True

from parser import parse_jsons_to_postgres


def search_messages(db_config, regex_patterns, window_size=60, min_matches=5):
    """
    Searches the PostgreSQL database for messages matching a list of regex patterns and finds windows of `window_size` seconds starting with the matching text.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.
        regex_patterns (list): List of regex patterns to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.

    Returns:
        tuple: (list, pd.Timestamp or None, int):
            - A list of dictionaries containing grouped search results.
            - The timestamp of the most recent live chat message in the database, or None if no messages.
            - The total number of lines searched (number of rows in the DataFrame).
    """
    # Create a connection string for pandas
    conn_str = f"postgresql://{db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Query to fetch messages and metadata
    query = """
        SELECT
            lc.timestamp,
            lc.video_id,
            ceil(extract(epoch from age(lc.timestamp, vm.release_timestamp))) as video_offset_time_seconds,
            lc.message,
            lc.author,
            lc.author_channel_id,
            vm.release_timestamp,
            vm.title,
            lc.video_offset_time_msec
        FROM live_chat lc
        JOIN video_metadata vm ON lc.video_id = vm.video_id;
    """

    # Use pandas to read directly from the database into a DataFrame
    df = pd.read_sql_query(query, conn_str)
    total_lines_searched = len(df)

    # Query for the latest live chat message in the database
    latest_live_chat_timestamp = None
    try:
        latest_chat_query = "SELECT MAX(timestamp) as latest_chat FROM live_chat;"
        latest_chat_df = pd.read_sql_query(latest_chat_query, conn_str)
        if not latest_chat_df.empty and pd.notnull(latest_chat_df.loc[0, 'latest_chat']):
            latest_live_chat_timestamp = latest_chat_df.loc[0, 'latest_chat']
    except Exception:
        latest_live_chat_timestamp = None

    # Ensure video_offset_time_seconds is an integer
    df["video_offset_time_seconds"] = (
        df["video_offset_time_seconds"].fillna(0).astype(int)
    )

    # Filter rows matching any of the regex patterns
    patterns = [re.compile(pattern) for pattern in regex_patterns]
    df["matches"] = df["message"].apply(
        lambda x: any(pattern.search(x) for pattern in patterns)
    )
    matching_df = df[df["matches"]]

    # Group matching rows by video_id and sort by timestamp
    grouped = matching_df.groupby("video_id", group_keys=False)
    results = []

    for video_id, group in grouped:
        group = group.sort_values("timestamp")
        for i, match_row in group.iterrows():
            start_time = match_row["timestamp"]
            end_time = start_time + pd.Timedelta(seconds=window_size)

            # Filter for matches within the window
            window_df = group[
                (group["timestamp"] >= start_time) & (group["timestamp"] < end_time)
            ]

            # Ensure min_matches and enforce window_size gap
            if len(window_df) >= min_matches:
                if not results or (
                    results[-1]["video_id"] != video_id
                    or (
                        match_row["timestamp"] - results[-1]["timestamp"]
                    ).total_seconds()
                    >= window_size
                ):
                    first_message = window_df.iloc[0].to_dict()
                    results.append(first_message)

    # Ensure results are sorted by timestamp (oldest to newest) before returning
    return sorted(results, key=lambda x: x["timestamp"]), latest_live_chat_timestamp, total_lines_searched


def count_missing_video_days(db_config):
    """
    Counts the number of days missing from the video_metadata table since 2024-05-25,
    exclusive of today. Returns the count and the list of missing dates.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.

    Returns:
        tuple: (int, list) The number of days missing video metadata and a list of missing dates.
               Returns (-1, []) in case of an error.
    """
    conn_str = f"postgresql://{db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Define the fixed earliest date to consider
    effective_start_of_period = pd.Timestamp('2024-05-25').date()

    # Calculate the date range, exclusive of today
    today_date = pd.Timestamp.today().date()
    # The period ends on yesterday
    end_of_period = today_date - pd.Timedelta(days=1)

    # Ensure end_of_period is not before effective_start_of_period
    if end_of_period < effective_start_of_period:
        print(f"The period ending yesterday ({end_of_period.strftime('%Y-%m-%d')}) is before the earliest allowed start date ({effective_start_of_period.strftime('%Y-%m-%d')}). No data to check.")
        return 0, []

    query = f"""
        SELECT DISTINCT CAST(release_timestamp AS DATE) as video_date
        FROM video_metadata
        WHERE release_timestamp >= '{effective_start_of_period}' AND release_timestamp < '{today_date}';
    """

    try:
        df = pd.read_sql_query(query, conn_str)
    except Exception as e:
        print(f"Error querying database: {e}")
        return -1, []  # Indicate an error

    # Generate all dates in the defined period for comparison
    all_period_dates = set(
        pd.date_range(effective_start_of_period, end_of_period, freq="D").date
    )

    if df.empty:
        # If no videos found in the defined period, all days are considered missing
        return len(all_period_dates), sorted(list(all_period_dates))

    # Convert database dates to a set for efficient lookup
    db_dates = set(pd.to_datetime(df["video_date"]).dt.date)

    missing_dates = sorted(list(all_period_dates - db_dates))
    return len(missing_dates), missing_dates


def get_video_offsets(db_config):
    """
    Builds a dictionary of video_id to offsets based on the minimum timestamp difference
    between live chat messages and video release timestamps.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.

    Returns:
        dict: A dictionary where keys are video_id and values are offsets in seconds.
    """
    # Create a connection string for pandas
    conn_str = f"postgresql://{db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Query to calculate offsets
    query = """
        SELECT
            vm.video_id,
            EXTRACT(EPOCH FROM MIN(lc.timestamp) - vm.release_timestamp) AS offset_seconds
        FROM live_chat lc
        JOIN video_metadata vm ON lc.video_id = vm.video_id
        WHERE lc.timestamp >= vm.release_timestamp
        GROUP BY vm.video_id;
    """

    # Use pandas to execute the query and build the dictionary
    df = pd.read_sql_query(query, conn_str)
    return dict(zip(df["video_id"], df["offset_seconds"].astype(int)))


def print_search_results_as_markdown(
    db_config,
    regex_patterns,
    window_size=60,
    min_matches=5,
    timestamp_offset=-10,
    output_file=None,
    debug=False,
):
    """
    Searches the database and prints results as a markdown table with columns:
    - Video Date (YYYY-mm-dd)
    - Video Title (as a YouTube link)
    - Timestamp Link (HH:MM:SS)
    Optionally includes Author and Message columns if debug is enabled.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.
        regex_patterns (list): List of regex patterns to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
        timestamp_offset (int): Number of seconds to subtract from the timestamp for context.
        output_file (str): Path to the file to write results to.
        debug (bool): Whether to include Author and Message columns in the output.
    """
    results, latest_live_chat_timestamp, total_lines_searched = search_messages(db_config, regex_patterns, window_size, min_matches)
    # offsets = get_video_offsets(db_config)

    # Define headers as a list based on debug mode
    headers = ["Date", "Title", "Timestamp"]
    if debug:
        headers.extend(["Author", "Message"])

    # Generate markdown header and spacer line dynamically
    header_line = f"| {' | '.join(headers)} |"
    spacer_line = f"|{'------|' * len(headers)}"

    output_lines = [header_line, spacer_line]

    for result in results:
        video_link = f"https://www.youtube.com/watch?v={result['video_id']}"

        # Use msec if available, otherwise use the computed offset from the database
        msec = result.get("video_offset_time_msec", 0)
        if msec > 0:
            result["video_offset_time_seconds"] = msec / 1000

        timestamp_adjusted_seconds = int(
            ceil(result["video_offset_time_seconds"] + timestamp_offset)
        )
        timestamp_link = f"{video_link}&t={timestamp_adjusted_seconds}s"
        timestamp_hms = pd.to_datetime(timestamp_adjusted_seconds, unit="s").strftime(
            "%H:%M:%S"
        )

        row = [
            f"{result['timestamp'].date()}",
            f"[{result['title']}]({video_link})",
            f"[{timestamp_hms}]({timestamp_link})",
        ]

        if debug:
            row.extend(
                [
                    result.get("author", ""),
                    result.get("message", ""),
                ]
            )

        output_lines.append(f"| {' | '.join(row)} |")

    # Escape characters in the regex patterns that might interfere with markdown display
    escaped_regex_patterns = [
        re.sub(r"([*_~|`])", r"\\\\\1", pattern) for pattern in regex_patterns
    ]

    # Add a summary table with search parameters
    output_lines.append("\n")
    output_lines.append("| Parameter       | Value |")
    output_lines.append("|-----------------|-------|")
    output_lines.append(f"| Search Patterns | `{', '.join(escaped_regex_patterns)}` |")
    output_lines.append(f"| Window Size     | {window_size} seconds |")
    output_lines.append(f"| Minimum Matches | {min_matches} |")
    output_lines.append(f"| Results Found   | {len(results)} |")
    output_lines.append(f"| Lines Searched  | {total_lines_searched} |")
    output_lines.append(
        f"| Generated At    | {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')} |"
    )

    # Add Latest Live Chat (from the database, as UTC)
    if latest_live_chat_timestamp is not None:
        latest_chat_utc = pd.Timestamp(latest_live_chat_timestamp).tz_localize(None).tz_localize('UTC') if pd.Timestamp(latest_live_chat_timestamp).tzinfo is None else pd.Timestamp(latest_live_chat_timestamp).tz_convert('UTC')
        latest_chat_str = latest_chat_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
        output_lines.append(f"| Latest Live Chat | {latest_chat_str} |")

    # Escape markdown output using rich's escape function
    escaped_output_lines = [escape(line) for line in output_lines]
    markdown_output = "\n".join(escaped_output_lines)
    print(Markdown(markdown_output))

    if output_file:
        with open(output_file, "w") as f:
            f.write(markdown_output)


def plot_unique_chatters_over_time(db_config, video_ids, window_size_minutes=5, output_file=None):
    """
    Creates an interactive histogram showing the count of unique chatters over the length of the stream
    in 5-minute windows. Uses Plotly for better emoji and text rendering support.

    Parameters:
        db_config (dict): Database configuration for PostgreSQL connection.
        video_ids (list): List of video IDs to analyze.
        window_size_minutes (int): Size of time windows in minutes.
        output_file (str): Path to save the plot image (HTML format). If None, opens in browser.
    """
    # Create a connection string for pandas
    conn_str = f"postgresql://{db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Build the placeholders and parameters for the SQL query
    placeholders = ",".join([f"'{vid}'" for vid in video_ids])

    # Query to fetch messages for the given video IDs
    query = f"""
        SELECT
            video_id,
            author,
            video_offset_time_msec
        FROM live_chat
        WHERE video_id IN ({placeholders})
        ORDER BY video_id, video_offset_time_msec;
    """

    try:
        df = pd.read_sql_query(query, conn_str)
    except Exception as e:
        print(f"[red]Error querying database: {e}[/red]")
        return

    if df.empty:
        print("[yellow]No chat messages found for the specified video IDs.[/yellow]")
        return

    # Query to fetch video metadata (titles and dates)
    metadata_query = f"""
        SELECT
            video_id,
            title,
            COALESCE(release_timestamp, timestamp) AS release_timestamp
        FROM video_metadata
        WHERE video_id IN ({placeholders});
    """

    try:
        metadata_df = pd.read_sql_query(metadata_query, conn_str)
        metadata_df['video_date'] = (
            pd.to_datetime(metadata_df['release_timestamp'], utc=True).dt.date
        )
        video_titles = dict(zip(metadata_df['video_id'], metadata_df['title']))
        video_dates = dict(zip(metadata_df['video_id'], metadata_df['video_date']))
    except Exception as e:
        print(f"[yellow]Warning: Could not fetch video titles: {e}[/yellow]")
        video_titles = {}
        video_dates = {}

    # Convert video_offset_time_msec to seconds
    df['video_offset_time_sec'] = df['video_offset_time_msec'] / 1000

    # Convert window size from minutes to seconds
    window_size_seconds = window_size_minutes * 60

    # Group by video_id and process each video separately
    results_per_video = {}

    for video_id in video_ids:
        video_df = df[df['video_id'] == video_id].copy()

        if video_df.empty:
            continue

        # Determine the maximum offset time (stream length)
        max_offset_sec = video_df['video_offset_time_sec'].max()

        # Create windows
        windows = np.arange(0, max_offset_sec + window_size_seconds, window_size_seconds)

        # Count unique chatters in each window
        unique_chatters_per_window = []
        window_labels = []

        for i in range(len(windows) - 1):
            window_start = windows[i]
            window_end = windows[i + 1]

            # Filter messages in this window
            window_messages = video_df[
                (video_df['video_offset_time_sec'] >= window_start) &
                (video_df['video_offset_time_sec'] < window_end)
            ]

            # Count unique authors
            unique_count = window_messages['author'].nunique()
            unique_chatters_per_window.append(unique_count)

            # Create label for this window (e.g., "0:00-5:00")
            start_hms = pd.to_datetime(window_start, unit='s').strftime('%H:%M:%S')
            end_hms = pd.to_datetime(window_end, unit='s').strftime('%H:%M:%S')
            window_labels.append(f"{start_hms}-{end_hms}")

        results_per_video[video_id] = {
            'counts': unique_chatters_per_window,
            'labels': window_labels,
            'max_offset': max_offset_sec,
            'title': video_titles.get(video_id, 'Unknown Title'),
            'date': video_dates.get(video_id, 'Unknown Date'),
            'url': f"https://www.youtube.com/watch?v={video_id}",
        }

    # Create interactive plot using Plotly
    num_videos = len(results_per_video)

    if num_videos == 0:
        print("[yellow]No chat messages found for the specified video IDs.[/yellow]")
        return
    elif num_videos == 1:
        # Single video: create a single histogram
        video_id = list(results_per_video.keys())[0]
        result = results_per_video[video_id]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=result['labels'],
            y=result['counts'],
            marker=dict(color='steelblue', line=dict(color='black', width=1.5)),
            hovertemplate='<b>%{x}</b><br>Unique Chatters: %{y}<extra></extra>',
            showlegend=False
        ))

        fig.update_layout(
            title=dict(
                text=(
                    f"<a href='{result['url']}' target='_blank'>{result['title']}</a>"
                    f"<br><sub>{result['date']} | {video_id}</sub>"
                ),
                font=dict(size=16)
            ),
            xaxis_title=f'Time Window ({window_size_minutes} min intervals)',
            yaxis_title='Unique Chatters',
            hovermode='x unified',
            template='plotly_white',
            height=600,
            xaxis=dict(tickangle=-45),
            margin=dict(b=120)
        )
    else:
        # Multiple videos: create subplots
        fig = make_subplots(
            rows=num_videos,
            cols=1,
            subplot_titles=[
                (
                    f"<a href='{result['url']}' target='_blank'>{result['title']}</a>"
                    f"<br>{result['date']} | {video_id}"
                )
                for video_id, result in results_per_video.items()
            ],
            specs=[[{"secondary_y": False}] for _ in range(num_videos)],
        )

        for idx, (video_id, result) in enumerate(results_per_video.items(), start=1):
            fig.add_trace(
                go.Bar(
                    x=result['labels'],
                    y=result['counts'],
                    marker=dict(color='steelblue', line=dict(color='black', width=1.5)),
                    hovertemplate='<b>%{x}</b><br>Unique Chatters: %{y}<extra></extra>',
                    showlegend=False,
                    name=video_id
                ),
                row=idx, col=1
            )

            fig.update_xaxes(title_text=f'Time Window ({window_size_minutes} min intervals)', row=idx, col=1, tickangle=-45)
            fig.update_yaxes(title_text='Unique Chatters', row=idx, col=1)

        fig.update_layout(
            height=500 * num_videos,
            showlegend=False,
            template='plotly_white',
            hovermode='x unified',
            margin=dict(b=120)
        )

    # Save or show the plot
    if output_file:
        # Ensure HTML extension
        if not output_file.endswith('.html'):
            output_file = output_file.replace('.png', '.html')
            if not output_file.endswith('.html'):
                output_file += '.html'
        fig.write_html(output_file)
        print(f"[green]Interactive plot saved to: {output_file}[/green]")
    else:
        fig.show()



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
        "regex_patterns",
        metavar="REGEX_PATTERNS",
        type=str,
        nargs="+",
        help="List of regex patterns to search for in messages.",
    )
    search_parser.add_argument(
        "-o",
        "--output-file",
        metavar="OUTPUT_FILE",
        type=str,
        help="File to write search results to.",
    )
    search_parser.add_argument(
        "--debug",
        action="store_true",
        help="Include Author and Message columns in the output.",
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

    # Missing days sub-command
    missing_days_parser = subparsers.add_parser(
        "missing_days", help="Count missing video metadata days since 2024-05-25."
    )
    # No arguments needed for missing_days anymore

    # Plot sub-command
    plot_parser = subparsers.add_parser(
        "plot", help="Create a histogram of unique chatters over stream duration."
    )
    plot_parser.add_argument(
        "video_ids",
        metavar="VIDEO_ID",
        type=str,
        nargs="+",
        help="List of video IDs to analyze.",
    )
    plot_parser.add_argument(
        "-w",
        "--window-size",
        metavar="MINUTES",
        type=int,
        default=5,
        help="Window size in minutes (default: 5).",
    )
    plot_parser.add_argument(
        "-o",
        "--output-file",
        metavar="OUTPUT_FILE",
        type=str,
        help="File to save the interactive plot to (HTML format).",
    )

    args = parser.parse_args()

    db_config = {
        "dbname": "ytlc",
        "user": "ytlc",
        "host": "localhost",
        "port": 5432,
    }

    if args.command == "search":
        print_search_results_as_markdown(
            db_config,
            args.regex_patterns,
            window_size=60,
            min_matches=5,
            timestamp_offset=-10,
            output_file=args.output_file,
            debug=args.debug,
        )

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

    elif args.command == "missing_days":
        missing_count, missing_dates = count_missing_video_days(db_config)
        if missing_count >= 0:
            # Determine the actual start date used for the report
            today_date_cli = pd.Timestamp.today().date()
            end_of_period_cli = today_date_cli - pd.Timedelta(days=1)
            effective_start_date_cli = pd.Timestamp('2024-05-25').date()

            if end_of_period_cli < effective_start_date_cli:
                 print(f"The period ending yesterday ({end_of_period_cli.strftime('%Y-%m-%d')}) is before the earliest allowed start date ({effective_start_date_cli.strftime('%Y-%m-%d')}). No data to check.")
            else:
                print(f"Found {missing_count} days missing video metadata in the period from {effective_start_date_cli.strftime('%Y-%m-%d')} to {end_of_period_cli.strftime('%Y-%m-%d')}.")
                if missing_dates:
                    print("Missing dates:")
                    for date_obj in missing_dates:
                        print(f"- {date_obj.strftime('%Y-%m-%d')}")
        else:
            print(f"Could not determine missing days due to an error.")

    elif args.command == "plot":
        plot_unique_chatters_over_time(
            db_config,
            args.video_ids,
            window_size_minutes=args.window_size,
            output_file=args.output_file,
        )


if __name__ == "__main__":
    main()

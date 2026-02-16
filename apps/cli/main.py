import json
import os
import pandas as pd
import re
import argparse
import psycopg2
from rich.markdown import Markdown
from rich.console import Console
from rich.markup import escape
from rich import print
from math import ceil
import numpy as np
from bokeh.plotting import figure, output_file, save, show
from bokeh.layouts import column
from bokeh.models import HoverTool, Range1d
from bokeh.io import output_file as bokeh_output_file

# Enable pandas copy-on-write mode for memory optimization
pd.options.mode.copy_on_write = True

from parser import parse_jsons_to_postgres


def get_db_connection_string():
    """
    Get a PostgreSQL connection string using libpq's standard precedence.
    This follows the same order as psql and other PostgreSQL tools:
    1. PG* environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
    2. ~/.pgpass file
    3. libpq defaults

    Returns:
        str: PostgreSQL connection string for use with pandas/SQLAlchemy.
    """
    # Let psycopg2/libpq resolve all parameters using standard precedence
    conn = psycopg2.connect()
    dsn_params = conn.get_dsn_parameters()
    conn.close()

    return f"postgresql://{dsn_params['user']}@{dsn_params['host']}:{dsn_params['port']}/{dsn_params['dbname']}"


def search_messages(regex_patterns, window_size=60, min_matches=5):
    """
    Searches the PostgreSQL database for messages matching a list of regex patterns and finds windows of `window_size` seconds starting with the matching text.

    Parameters:
        regex_patterns (list): List of regex patterns to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.

    Returns:
        tuple: (list, pd.Timestamp or None, int):
            - A list of dictionaries containing grouped search results.
            - The timestamp of the most recent live chat message in the database, or None if no messages.
            - The total number of lines searched (number of rows in the DataFrame).
    """
    # Get connection string using libpq's standard precedence (same as psql)
    conn_str = get_db_connection_string()

    # Build PostgreSQL regex filter conditions (case-insensitive)
    # Note: PostgreSQL regex syntax (~*) is close to Python but not identical
    # This filters at the database level using the gin_trgm_ops index for performance
    regex_conditions = " OR ".join([
        f"lc.message ~* '{pattern.replace(chr(39), chr(39)*2)}'"  # Escape single quotes
        for pattern in regex_patterns
    ])

    # Query to fetch messages and metadata with database-level filtering
    # This uses the trigram index to efficiently find matching messages
    query = f"""
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
        JOIN video_metadata vm ON lc.video_id = vm.video_id
        WHERE {regex_conditions}
        ORDER BY lc.video_id, lc.timestamp;
    """

    # Use pandas to read directly from the database into a DataFrame
    # This only fetches rows matching the regex patterns, not the entire table
    db_filtered = False
    try:
        df = pd.read_sql_query(query, conn_str)
        db_filtered = True
    except Exception as e:
        # If PostgreSQL regex fails, fall back to fetching all rows
        print(f"Database-level regex filtering failed: {e}")
        print("Falling back to Python-level filtering (this may be slower)...")
        query_fallback = """
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
        df = pd.read_sql_query(query_fallback, conn_str)

    # Query for the total number of messages in the database (what was actually searched)
    try:
        total_lines_query = "SELECT COUNT(*) as total_count FROM live_chat;"
        total_lines_df = pd.read_sql_query(total_lines_query, conn_str)
        total_lines_searched = int(total_lines_df.loc[0, 'total_count']) if not total_lines_df.empty else len(df)
    except Exception:
        # Fallback to the length of the dataframe
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

    # If database-level filtering failed, apply Python regex filtering now
    if not db_filtered:
        patterns = [re.compile(pattern) for pattern in regex_patterns]
        df["matches"] = df["message"].apply(
            lambda x: any(pattern.search(x) for pattern in patterns)
        )
        df = df[df["matches"]]

    # Group matching rows by video_id and sort by timestamp
    grouped = df.groupby("video_id", group_keys=False)
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


def count_missing_video_days():
    """
    Counts the number of days missing from the video_metadata table since 2024-05-25,
    exclusive of today. Returns the count and the list of missing dates.

    Returns:
        tuple: (int, list) The number of days missing video metadata and a list of missing dates.
               Returns (-1, []) in case of an error.
    """
    conn_str = get_db_connection_string()

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


def get_video_offsets():
    """
    Builds a dictionary of video_id to offsets based on the minimum timestamp difference
    between live chat messages and video release timestamps.

    Returns:
        dict: A dictionary where keys are video_id and values are offsets in seconds.
    """
    # Get connection string using libpq's standard precedence
    conn_str = get_db_connection_string()

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
        regex_patterns (list): List of regex patterns to search for in messages.
        window_size (int): Time window size in seconds for grouping messages.
        min_matches (int): Minimum number of matches required within a time window.
        timestamp_offset (int): Number of seconds to subtract from the timestamp for context.
        output_file (str): Path to the file to write results to.
        debug (bool): Whether to include Author and Message columns in the output.
    """
    results, latest_live_chat_timestamp, total_lines_searched = search_messages(regex_patterns, window_size, min_matches)
    # offsets = get_video_offsets()

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


def plot_unique_chatters_over_time(
    video_ids,
    window_size_minutes=5,
    output_file=None,
    last_n=None,
    start_date=None,
    end_date=None,
):
    """
    Creates an interactive histogram showing the count of unique chatters over the length of the stream
    in 5-minute windows. Uses Bokeh for better support of multiple independent plots.

    Parameters:
        video_ids (list): List of video IDs to analyze.
        window_size_minutes (int): Size of time windows in minutes.
        output_file (str): Path to save the plot image (HTML format). If None, opens in browser.
        last_n (int or None): Plot the most recent N VODs when video_ids is empty.
        start_date (str or None): Filter VODs from this date (YYYY-MM-DD) when video_ids is empty.
        end_date (str or None): Filter VODs up to this date (YYYY-MM-DD) when video_ids is empty.
    """
    # Get connection string using libpq's standard precedence
    conn_str = get_db_connection_string()

    # Resolve video IDs from metadata when not provided
    if not video_ids:
        if last_n is not None and (start_date or end_date):
            print("[red]Use either --last-n or --start-date/--end-date, not both.[/red]")
            return

        start_date_value = None
        end_date_value = None
        try:
            if start_date:
                start_date_value = pd.to_datetime(start_date).date()
            if end_date:
                end_date_value = pd.to_datetime(end_date).date()
        except Exception:
            print("[red]Invalid date format. Use YYYY-MM-DD.[/red]")
            return

        if last_n is not None:
            if last_n <= 0:
                print("[red]--last-n must be a positive integer.[/red]")
                return

        where_clauses = []
        if start_date_value:
            where_clauses.append(
                f"COALESCE(release_timestamp, timestamp)::date >= '{start_date_value}'"
            )
        if end_date_value:
            where_clauses.append(
                f"COALESCE(release_timestamp, timestamp)::date <= '{end_date_value}'"
            )

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = f"LIMIT {int(last_n)}" if last_n else ""

        video_query = f"""
            SELECT video_id
            FROM video_metadata
            {where_sql}
            ORDER BY COALESCE(release_timestamp, timestamp) DESC
            {limit_sql};
        """

        try:
            video_df = pd.read_sql_query(video_query, conn_str)
        except Exception as e:
            print(f"[red]Error querying video metadata: {e}[/red]")
            return

        video_ids = video_df["video_id"].dropna().tolist()
        if not video_ids:
            print("[yellow]No videos found for the selected criteria.[/yellow]")
            return

    # Build the placeholders and parameters for the SQL query
    placeholders = ",".join([f"'{vid}'" for vid in video_ids])

    # Query to fetch messages for the given video IDs
    query = f"""
        SELECT
            video_id,
            author,
            video_offset_time_msec,
            message
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
            EXTRACT(EPOCH FROM duration) * 1000 AS duration_msec,
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
        video_durations = dict(zip(metadata_df['video_id'], metadata_df['duration_msec']))
    except Exception as e:
        print(f"[yellow]Warning: Could not fetch video titles: {e}[/yellow]")
        video_titles = {}
        video_dates = {}
        video_durations = {}

    # Convert video_offset_time_msec to seconds
    df['video_offset_time_sec'] = df['video_offset_time_msec'] / 1000

    # Filter messages beyond video duration if available
    filtered_df_list = []
    for video_id in df['video_id'].unique():
        video_df = df[df['video_id'] == video_id]
        duration_msec = video_durations.get(video_id)
        if duration_msec is not None and not pd.isna(duration_msec):
            video_df = video_df[video_df['video_offset_time_msec'] <= duration_msec]
        filtered_df_list.append(video_df)

    if filtered_df_list:
        df = pd.concat(filtered_df_list, ignore_index=True)
    else:
        df = pd.DataFrame()

    # Convert window size from minutes to seconds
    window_size_seconds = window_size_minutes * 60

    # Group by video_id and process each video separately
    results_per_video = {}

    def video_sort_key(vid):
        video_date = video_dates.get(vid)
        if video_date is None:
            return (1, pd.Timestamp.max.date(), vid)
        return (0, video_date, vid)

    ordered_video_ids = sorted(video_ids, key=video_sort_key)

    def extract_emojis(text):
        """Extract custom emoji names in the format :_Kanna*: from text."""
        if not text:
            return []
        # Pattern to match custom emojis like :_KannaLove:, :_KannaHappy:, etc.
        emoji_pattern = re.compile(r':_[^:]+:')
        return emoji_pattern.findall(text)

    for video_id in ordered_video_ids:
        video_df = df[df['video_id'] == video_id].copy()

        if video_df.empty:
            continue

        # Determine the maximum offset time (stream length)
        max_offset_sec = video_df['video_offset_time_sec'].max()

        # Create windows
        windows = np.arange(0, max_offset_sec + window_size_seconds, window_size_seconds)

        # Count unique chatters and messages in each window
        unique_chatters_per_window = []
        messages_per_window = []
        window_labels = []
        top_emojis = []

        for i in range(len(windows) - 1):
            window_start = windows[i]
            window_end = windows[i + 1]

            # Filter messages in this window
            window_messages = video_df[
                (video_df['video_offset_time_sec'] >= window_start) &
                (video_df['video_offset_time_sec'] < window_end)
            ]

            # Count unique authors and total messages
            unique_count = window_messages['author'].nunique()
            message_count = len(window_messages)
            unique_chatters_per_window.append(unique_count)
            messages_per_window.append(message_count)

            # Extract and count emojis
            all_emojis = []
            for msg in window_messages['message'].dropna():
                all_emojis.extend(extract_emojis(str(msg)))

            # Find most common emoji
            if all_emojis:
                from collections import Counter
                emoji_counts = Counter(all_emojis)
                most_common_emoji = emoji_counts.most_common(1)[0][0]
                top_emojis.append(most_common_emoji)
            else:
                top_emojis.append('')

            # Create label for this window (e.g., "0:00-5:00")
            start_hms = pd.to_datetime(window_start, unit='s').strftime('%H:%M:%S')
            end_hms = pd.to_datetime(window_end, unit='s').strftime('%H:%M:%S')
            window_labels.append(f"{start_hms}-{end_hms}")

        video_date = video_dates.get(video_id)
        date_label = video_date.isoformat() if video_date else 'Unknown Date'

        total_unique_chatters = video_df['author'].nunique()
        total_messages = len(video_df)

        results_per_video[video_id] = {
            'counts': unique_chatters_per_window,
            'messages': messages_per_window,
            'labels': window_labels,
            'top_emojis': top_emojis,
            'max_offset': max_offset_sec,
            'title': video_titles.get(video_id, 'Unknown Title'),
            'date': date_label,
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'total_unique': int(total_unique_chatters),
            'total_messages': int(total_messages),
        }

    # Create interactive plots using Bokeh
    num_videos = len(results_per_video)

    if num_videos == 0:
        print("[yellow]No chat messages found for the specified video IDs.[/yellow]")
        return

    # Create a list to hold all figures
    figures = []

    for video_id, result in results_per_video.items():
        # Format time labels for x-axis (show every nth label to avoid crowding)
        x_indices = list(range(len(result['labels'])))
        x_labels = result['labels']

        # Determine tick spacing based on number of windows
        num_windows = len(x_labels)
        if num_windows > 50:
            tick_interval = num_windows // 20
        elif num_windows > 20:
            tick_interval = num_windows // 10
        else:
            tick_interval = max(1, num_windows // 10)

        # Create figure with dual y-axes
        p = figure(
            width=1200,
            height=400,
            title=f"{result['title']}\n{result['date']} | {result['total_unique']} unique chatters | {result['total_messages']} messages",
            x_axis_label=f"Time Window ({window_size_minutes} min intervals)",
            y_axis_label="Unique Chatters",
            toolbar_location="above",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            output_backend="svg",
            x_range=(-0.5, len(x_indices) - 0.5),
        )

        # Remove padding around the plot
        p.min_border_left = 0
        p.min_border_right = 0
        p.min_border_top = 0
        p.min_border_bottom = 0

        # Configure x-axis
        p.xaxis.ticker = list(range(0, len(x_labels), tick_interval))
        p.xaxis.major_label_overrides = {i: x_labels[i] for i in range(0, len(x_labels), tick_interval)}
        p.xaxis.major_label_orientation = 0.785  # 45 degrees in radians

        # Create unified data source with all information
        from bokeh.models import ColumnDataSource, LinearAxis, TapTool

        # Set up the secondary y-axis for messages
        max_chatters = max(result['counts']) if result['counts'] else 1
        max_messages = max(result['messages']) if result['messages'] else 1

        # Create a scaling factor for the secondary axis
        scaling_factor = max_chatters / max_messages if max_messages > 0 else 1
        scaled_messages = [m * scaling_factor for m in result['messages']]

        # Create URLs for each time window (pointing to YouTube video at that timestamp)
        urls = []
        for i, label in enumerate(x_labels):
            # Parse the start time from the label (format: "HH:MM:SS-HH:MM:SS")
            start_time_str = label.split('-')[0]
            # Convert to seconds
            h, m, s = map(int, start_time_str.split(':'))
            timestamp_seconds = h * 3600 + m * 60 + s
            urls.append(f"{result['url']}&t={timestamp_seconds}s")

        # Convert emoji names to image URLs
        emoji_image_urls = []
        for emoji in result['top_emojis']:
            if emoji:
                # Remove colons and add .png extension
                emoji_filename = emoji.strip(':') + '.png'
                # Use relative path to img directory
                emoji_image_urls.append(f"img/{emoji_filename}")
            else:
                # Empty string for no emoji
                emoji_image_urls.append('')

        # Calculate emoji size in screen pixels to maintain square aspect ratio
        # Bar width is 0.8 data units, plot is 1200 pixels wide
        bar_width_pixels = int((0.8 / num_windows) * 1200)
        # Use at least 20 pixels, max 60 pixels for emoji
        emoji_size_pixels = max(20, min(60, bar_width_pixels))

        # Calculate emoji y positions (center of image above each bar) in data coordinates
        # Position emoji right on top of bars (at bar height)
        emoji_y_positions = result['counts']

        # Unified data source with all information
        source = ColumnDataSource(data=dict(
            x=x_indices,
            time_label=x_labels,
            chatters=result['counts'],
            messages=result['messages'],
            scaled_messages=scaled_messages,
            url=urls,
            top_emoji=result['top_emojis'],
            emoji_image_url=emoji_image_urls,
            emoji_y=emoji_y_positions
        ))

        # Add bars for unique chatters
        bars = p.vbar(
            x='x',
            top='chatters',
            width=0.8,
            source=source,
            color='#2E7D32',  # Forest green from image
            line_color='black',
            line_width=1.5,
            legend_label='Unique Chatters',
            nonselection_fill_alpha=1.0,
            nonselection_fill_color='#2E7D32',
            nonselection_line_alpha=1.0,
            nonselection_line_color='black',
            selection_fill_alpha=1.0,  # Keep selected bars same opacity
            selection_fill_color='#2E7D32',
            selection_line_alpha=1.0,
            selection_line_color='black',
        )

        # Add emoji images at fixed square size in screen pixels
        p.image_url(
            url='emoji_image_url',
            x='x',
            y='emoji_y',
            w=emoji_size_pixels,
            h=emoji_size_pixels,
            source=source,
            anchor='bottom',  # Anchor at bottom so emoji sits right on top of bar
            w_units='screen',  # Fixed screen pixels
            h_units='screen'   # Fixed screen pixels - maintains perfect square
        )

        # Add line for messages (scaled to fit with bars)
        line = p.line(
            'x',
            'scaled_messages',
            source=source,
            line_width=2,
            color='#FFD700',  # Golden yellow from image
            legend_label='Messages',
            nonselection_line_alpha=1.0,
            nonselection_line_color='#FFD700',
            selection_line_alpha=1.0,  # Keep selected line same opacity
            selection_line_color='#FFD700',
        )

        # Add circle markers on the line
        circles = p.scatter(
            'x',
            'scaled_messages',
            source=source,
            size=6,
            color='#FFD700',  # Golden yellow from image
            marker='circle',
            nonselection_fill_alpha=1.0,
            nonselection_fill_color='#FFD700',
            nonselection_line_alpha=1.0,
            selection_fill_alpha=1.0,  # Keep selected circles same opacity
            selection_fill_color='#FFD700',
            selection_line_alpha=1.0,
        )

        # Add secondary y-axis label using extra_y_ranges
        p.extra_y_ranges = {"messages": Range1d(start=0, end=max_messages * 1.1)}
        p.add_layout(LinearAxis(y_range_name="messages", axis_label="Messages"), 'right')

        # Add unified hover tool showing both metrics (attach only to bars to avoid duplicates)
        hover = HoverTool(
            renderers=[bars],
            tooltips=[
                ("Time", "@time_label"),
                ("Unique Chatters", "@chatters"),
                ("Messages", "@messages")
            ],
            mode='vline'
        )

        p.add_tools(hover)

        # Add tap tool with CustomJS to open URLs without affecting selection state
        from bokeh.models import CustomJS
        tap_tool = TapTool()
        tap_tool.callback = CustomJS(args=dict(source=source), code="""
            const data = source.data;
            const index = source.selected.indices[0];
            if (index !== undefined) {
                const url = data['url'][index];
                window.open(url, '_blank');
                // Clear selection to prevent visual artifacts
                source.selected.indices = [];
            }
        """)
        p.add_tools(tap_tool)

        # Configure legend
        p.legend.location = "top_right"
        p.legend.click_policy = "hide"

        # Make the title a clickable link (add to title as HTML won't work, so we'll keep it simple)
        p.title.text_font_size = "14pt"

        figures.append(p)

    # Combine all figures into a column layout
    layout = column(*figures)

    # Save or show the plot
    if output_file:
        # Ensure HTML extension
        if not output_file.endswith('.html'):
            output_file = output_file.replace('.png', '.html')
            if not output_file.endswith('.html'):
                output_file += '.html'
        bokeh_output_file(output_file)
        save(layout)
        print(f"[green]Interactive plot saved to: {output_file}[/green]")
    else:
        show(layout)



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
        "data_dir",
        metavar="DATA_DIR",
        type=str,
        help="Directory path containing both info and live chat JSON files to parse.",
    )

    # Plot sub-command
    plot_parser = subparsers.add_parser(
        "plot", help="Create a histogram of unique chatters over stream duration."
    )
    plot_parser.add_argument(
        "video_ids",
        metavar="VIDEO_ID",
        type=str,
        nargs="*",
        help="List of video IDs to analyze (omit when using --last-n or date filters).",
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
    plot_parser.add_argument(
        "--last-n",
        metavar="COUNT",
        type=int,
        help="Plot the most recent N VODs by release date.",
    )
    plot_parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        type=str,
        help="Filter VODs from this date (inclusive).",
    )
    plot_parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        type=str,
        help="Filter VODs up to this date (inclusive).",
    )

    args = parser.parse_args()

    if args.command == "search":
        print_search_results_as_markdown(
            args.regex_patterns,
            window_size=60,
            min_matches=5,
            timestamp_offset=-10,
            output_file=args.output_file,
            debug=args.debug,
        )

    elif args.command == "parse":
        if not os.path.isdir(args.data_dir):
            parse_parser.error(
                f"Directory not found at {args.data_dir}"
            )

        print(f"Parsing JSON files from: {args.data_dir}")
        parse_jsons_to_postgres(
            args.data_dir, json_type="info"
        )
        parse_jsons_to_postgres(
            args.data_dir, json_type="live_chat"
        )

    elif args.command == "plot":
        if not args.video_ids and not (args.last_n or args.start_date or args.end_date):
            plot_parser.error(
                "Provide VIDEO_IDs or use --last-n/--start-date/--end-date to select videos."
            )
        plot_unique_chatters_over_time(
            args.video_ids,
            window_size_minutes=args.window_size,
            output_file=args.output_file,
            last_n=args.last_n,
            start_date=args.start_date,
            end_date=args.end_date,
        )


if __name__ == "__main__":
    main()

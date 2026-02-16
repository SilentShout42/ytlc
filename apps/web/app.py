import os
from flask import Flask, render_template, abort, request, make_response
import pandas as pd
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, LinearAxis, Range1d, TapTool, CustomJS
from bokeh.layouts import column
import numpy as np
import re
from collections import Counter
import psycopg2

app = Flask(__name__)


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


def extract_emojis(text):
    """Extract custom emoji names in the format :_Kanna*: from text."""
    if not text:
        return []
    emoji_pattern = re.compile(r':_[^:]+:')
    return emoji_pattern.findall(text)


def generate_bokeh_plot(video_id, window_size_minutes=5, exclude_global_top_emoji=True):
    """
    Generate a Bokeh plot for a specific video ID showing unique chatters over time.
    Returns (script, div) tuple for embedding in HTML template.
    """
    conn_str = get_db_connection_string()

    # Query to fetch messages for the given video ID
    query = f"""
        SELECT
            video_id,
            author,
            video_offset_time_msec,
            message
        FROM live_chat
        WHERE video_id = '{video_id}'
          AND video_offset_time_msec > 0
        ORDER BY video_offset_time_msec;
    """

    try:
        df = pd.read_sql_query(query, conn_str)
    except Exception as e:
        print(f"Error querying database: {e}")
        return None, None, None

    if df.empty:
        return None, None, None

    # Query to fetch video metadata
    metadata_query = f"""
        SELECT
            video_id,
            title,
            EXTRACT(EPOCH FROM duration) * 1000 AS duration_msec,
            COALESCE(release_timestamp, timestamp) AS release_timestamp
        FROM video_metadata
        WHERE video_id = '{video_id}';
    """

    try:
        metadata_df = pd.read_sql_query(metadata_query, conn_str)
        video_title = metadata_df.iloc[0]['title'] if not metadata_df.empty else 'Unknown Title'
        video_date = pd.to_datetime(metadata_df.iloc[0]['release_timestamp'], utc=True).date() if not metadata_df.empty else None
        video_duration_msec = metadata_df.iloc[0]['duration_msec'] if not metadata_df.empty and pd.notna(metadata_df.iloc[0]['duration_msec']) else None
    except Exception as e:
        print(f"Warning: Could not fetch video metadata: {e}")
        video_title = 'Unknown Title'
        video_date = None
        video_duration_msec = None

    # Convert video_offset_time_msec to seconds
    df['video_offset_time_sec'] = df['video_offset_time_msec'] / 1000

    # Filter messages beyond video duration if available
    if video_duration_msec is not None and not df.empty:
        df = df[df['video_offset_time_msec'] <= video_duration_msec]

    # Convert window size from minutes to seconds
    window_size_seconds = window_size_minutes * 60

    # Determine the maximum offset time (stream length)
    max_offset_sec = df['video_offset_time_sec'].max()

    # Create windows
    windows = np.arange(0, max_offset_sec + window_size_seconds, window_size_seconds)

    # Determine overall top emoji to exclude from per-window candidates
    all_stream_emojis = []
    for msg in df['message'].dropna():
        all_stream_emojis.extend(extract_emojis(str(msg)))
    global_top_emoji = None
    if all_stream_emojis:
        global_top_emoji = Counter(all_stream_emojis).most_common(1)[0][0]

    # Count unique chatters and messages in each window
    unique_chatters_per_window = []
    messages_per_window = []
    window_labels = []
    top_emojis = []

    for i in range(len(windows) - 1):
        window_start = windows[i]
        window_end = windows[i + 1]

        # Filter messages in this window
        window_messages = df[
            (df['video_offset_time_sec'] >= window_start) &
            (df['video_offset_time_sec'] < window_end)
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

        # Find most common emoji, excluding the overall top emoji
        if exclude_global_top_emoji and global_top_emoji:
            all_emojis = [emoji for emoji in all_emojis if emoji != global_top_emoji]

        if all_emojis:
            emoji_counts = Counter(all_emojis)
            most_common_emoji = emoji_counts.most_common(1)[0][0]
            top_emojis.append(most_common_emoji)
        else:
            top_emojis.append('')

        # Create label for this window using the start time (e.g., "0:00")
        start_hms = pd.to_datetime(window_start, unit='s').strftime('%H:%M:%S')
        window_labels.append(start_hms)

    total_unique_chatters = df['author'].nunique()
    total_messages = len(df)

    # Prepare data for plotting
    x_indices = list(range(len(window_labels)))
    num_windows = len(window_labels)

    # Determine tick spacing based on number of windows
    if num_windows > 50:
        tick_interval = num_windows // 20
    elif num_windows > 20:
        tick_interval = num_windows // 10
    else:
        tick_interval = max(1, num_windows // 10)

    # Create URLs and timestamp data for each time window
    urls = []
    timestamps = []
    for i, label in enumerate(window_labels):
        start_time_str = label.split('-')[0]
        h, m, s = map(int, start_time_str.split(':'))
        timestamp_seconds = h * 3600 + m * 60 + s
        urls.append(f"https://www.youtube.com/watch?v={video_id}&t={timestamp_seconds}s")
        timestamps.append(timestamp_seconds)

    # Convert emoji names to image URLs
    emoji_image_urls = []
    for emoji in top_emojis:
        if emoji:
            emoji_filename = emoji.strip(':') + '.png'
            emoji_image_urls.append(f"/static/img/{emoji_filename}")
        else:
            emoji_image_urls.append('')

    # Calculate emoji size
    bar_width_pixels = int((0.8 / num_windows) * 1200) if num_windows > 0 else 20
    emoji_size_pixels = max(20, min(60, bar_width_pixels))
    emoji_y_positions = unique_chatters_per_window

    # Create figure
    p = figure(
        width=1200,
        height=400,
        title=None,
        x_axis_label=None,
        y_axis_label=None,
        toolbar_location=None,
        tools="",
        sizing_mode="fixed",
        output_backend="svg",
    )

    # Configure x-axis
    p.xaxis.ticker = list(range(0, len(window_labels), tick_interval))
    p.xaxis.major_label_overrides = {i: window_labels[i] for i in range(0, len(window_labels), tick_interval)}
    p.xaxis.major_label_orientation = 0.785  # 45 degrees in radians

    # Set up the secondary y-axis for messages
    max_chatters = max(unique_chatters_per_window) if unique_chatters_per_window else 1
    max_messages = max(messages_per_window) if messages_per_window else 1

    # Create a scaling factor for the secondary axis
    scaling_factor = max_chatters / max_messages if max_messages > 0 else 1
    scaled_messages = [m * scaling_factor for m in messages_per_window]

    # Unified data source
    source = ColumnDataSource(data=dict(
        x=x_indices,
        time_label=window_labels,
        chatters=unique_chatters_per_window,
        messages=messages_per_window,
        scaled_messages=scaled_messages,
        url=urls,
        timestamp=timestamps,
        top_emoji=top_emojis,
        emoji_image_url=emoji_image_urls,
        emoji_y=emoji_y_positions
    ))

    # Add bars for unique chatters
    bars = p.vbar(
        x='x',
        top='chatters',
        width=0.8,
        source=source,
        color='#2E7D32',
        line_color='black',
        line_width=1.5,
        legend_label='Unique Chatters',
        nonselection_fill_alpha=1.0,
        nonselection_fill_color='#2E7D32',
        nonselection_line_alpha=1.0,
        nonselection_line_color='black',
        selection_fill_alpha=1.0,
        selection_fill_color='#2E7D32',
        selection_line_alpha=1.0,
        selection_line_color='black',
    )

    # Add emoji images
    p.image_url(
        url='emoji_image_url',
        x='x',
        y='emoji_y',
        w=emoji_size_pixels,
        h=emoji_size_pixels,
        source=source,
        anchor='bottom',
        w_units='screen',
        h_units='screen'
    )

    # Add line for messages (scaled to fit with bars)
    line = p.line(
        'x',
        'scaled_messages',
        source=source,
        line_width=2,
        color='#FFD700',  # Golden yellow
        legend_label='Messages',
        nonselection_line_alpha=1.0,
        nonselection_line_color='#FFD700',
        selection_line_alpha=1.0,
        selection_line_color='#FFD700',
    )

    # Add circle markers on the line
    circles = p.scatter(
        'x',
        'scaled_messages',
        source=source,
        size=6,
        color='#FFD700',
        marker='circle',
        nonselection_fill_alpha=1.0,
        nonselection_fill_color='#FFD700',
        nonselection_line_alpha=1.0,
        selection_fill_alpha=1.0,
        selection_fill_color='#FFD700',
        selection_line_alpha=1.0,
    )

    # Add secondary y-axis label using extra_y_ranges
    p.extra_y_ranges = {"messages": Range1d(start=0, end=max_messages * 1.1)}
    p.add_layout(LinearAxis(y_range_name="messages", axis_label=None), 'right')
    p.yaxis.visible = False
    if p.right:
        p.right[0].visible = False

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

    # Add tap tool with CustomJS to seek the YouTube player
    tap_tool = TapTool()
    tap_tool.callback = CustomJS(args=dict(source=source), code="""
        const data = source.data;
        const index = source.selected.indices[0];
        if (index !== undefined) {
            const timestamp = data['timestamp'][index];
            // Control the YouTube player
            if (typeof player !== 'undefined' && player.seekTo) {
                player.seekTo(timestamp, true);
                player.playVideo();
            }
            // Clear selection to prevent visual artifacts
            source.selected.indices = [];
        }
    """)
    p.add_tools(tap_tool)

    # Configure legend
    p.legend.visible = False

    # Return script and div for embedding
    script, div = components(p)
    return script, div, global_top_emoji


@app.route('/')
def index():
    """Display a list of all videos with titles and air dates."""
    conn_str = get_db_connection_string()

    query = """
        SELECT
            video_id,
            title,
            COALESCE(release_timestamp, timestamp) AS release_timestamp
        FROM video_metadata
        ORDER BY COALESCE(release_timestamp, timestamp) DESC;
    """

    try:
        df = pd.read_sql_query(query, conn_str)
        df['date'] = pd.to_datetime(df['release_timestamp'], utc=True).dt.date
        videos = df.to_dict('records')
    except Exception as e:
        print(f"Error querying database: {e}")
        videos = []

    return render_template('index.html', videos=videos)


@app.route('/video/<video_id>')
def video_detail(video_id):
    """Display a video detail page with a Bokeh plot."""
    conn_str = get_db_connection_string()

    # Fetch video metadata
    query = f"""
        SELECT
            video_id,
            title,
            COALESCE(release_timestamp, timestamp) AS release_timestamp
        FROM video_metadata
        WHERE video_id = '{video_id}';
    """

    try:
        df = pd.read_sql_query(query, conn_str)
        if df.empty:
            abort(404)
        video = df.iloc[0].to_dict()
        video['date'] = pd.to_datetime(video['release_timestamp'], utc=True).date()
    except Exception as e:
        print(f"Error querying database: {e}")
        abort(404)

    exclude_param = request.args.get("exclude_global_top")
    if exclude_param is None:
        exclude_param = request.cookies.get("exclude_global_top", "1")
    exclude_global_top_emoji = exclude_param != "0"

    # Generate Bokeh plot
    script, div, global_top_emoji = generate_bokeh_plot(
        video_id,
        exclude_global_top_emoji=exclude_global_top_emoji,
    )

    if script is None or div is None:
        # No chat data available
        script = ""
        div = "<p>No chat data available for this video.</p>"
        global_top_emoji = None

    global_top_emoji_url = None
    if global_top_emoji:
        emoji_filename = global_top_emoji.strip(':') + '.png'
        global_top_emoji_url = f"/static/img/{emoji_filename}"

    response = make_response(
        render_template(
            'video.html',
            video=video,
            plot_script=script,
            plot_div=div,
            exclude_global_top_emoji=exclude_global_top_emoji,
            global_top_emoji=global_top_emoji,
            global_top_emoji_url=global_top_emoji_url,
        )
    )
    response.set_cookie("exclude_global_top", "1" if exclude_global_top_emoji else "0")
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

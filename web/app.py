"""
Flask web application for searching and visualizing YouTube live chat streams.
"""
import os
import logging
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
from datetime import datetime
import tempfile
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.layouts import column
from bokeh.models import HoverTool, Range1d, ColumnDataSource, LinearAxis, TapTool, CustomJS
from sqlalchemy import create_engine, text
import traceback

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("PGDATABASE", "ytlc"),
    "user": os.getenv("PGUSER", "ytlc"),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
}

# Create SQLAlchemy engine
_engine = None

def get_db_engine():
    """Get SQLAlchemy engine for database connections."""
    global _engine
    if _engine is None:
        conn_str = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        logger.info(f"Creating database engine with connection string: postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        _engine = create_engine(conn_str, pool_pre_ping=True, echo=True)
    return _engine


@app.route('/')
def index():
    """Render the main page with search interface."""
    return render_template('index.html')


@app.route('/api/search', methods=['GET'])
def search_streams():
    """
    Search for streams by title or date range.

    Query parameters:
    - title: Search term for video title (case-insensitive)
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - limit: Maximum number of results (default: 50)
    """
    title = request.args.get('title', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    limit = request.args.get('limit', '50')

    logger.debug(f"Search request - title: '{title}', start_date: '{start_date}', end_date: '{end_date}', limit: '{limit}'")

    try:
        limit = int(limit)
    except ValueError:
        limit = 50

    # Build the SQL query
    conditions = []
    if title:
        conditions.append(f"title ILIKE '%{title.replace(chr(39), chr(39)*2)}%'")

    if start_date:
        try:
            pd.to_datetime(start_date)
            conditions.append(f"release_timestamp >= '{start_date}'")
        except ValueError:
            return jsonify({"error": "Invalid start_date format. Use YYYY-MM-DD"}), 400

    if end_date:
        try:
            pd.to_datetime(end_date)
            conditions.append(f"release_timestamp <= '{end_date} 23:59:59'")
        except ValueError:
            return jsonify({"error": "Invalid end_date format. Use YYYY-MM-DD"}), 400

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            video_id,
            title,
            channel_name,
            release_timestamp,
            duration,
            was_live
        FROM video_metadata
        WHERE {where_clause}
        ORDER BY release_timestamp DESC
        LIMIT {limit};
    """

    try:
        logger.debug(f"Executing query: {query}")
        engine = get_db_engine()
        logger.debug("Got database engine")

        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
            logger.debug(f"Query returned {len(df)} rows")

        # Convert to JSON-serializable format
        results = []
        for idx in range(len(df)):
            row = df.iloc[idx]
            results.append({
                'video_id': row['video_id'],
                'title': row['title'],
                'channel_name': row['channel_name'],
                'release_timestamp': row['release_timestamp'].isoformat() if pd.notna(row['release_timestamp']) else None,
                'duration': str(row['duration']) if pd.notna(row['duration']) else None,
                'was_live': bool(row['was_live']) if pd.notna(row['was_live']) else False
            })

        logger.debug(f"Successfully converted {len(results)} results to JSON format")
        return jsonify({
            'success': True,
            'count': len(results),
            'results': results
        })

    except Exception as e:
        logger.error(f"Error in search_streams: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/plot/<video_id>', methods=['GET'])
def generate_plot(video_id):
    """
    Generate a Bokeh plot for the specified video ID.

    Query parameters:
    - window_size: Window size in minutes (default: 5)
    """
    window_size_minutes = request.args.get('window_size', '5')
    try:
        window_size_minutes = int(window_size_minutes)
    except ValueError:
        window_size_minutes = 5

    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Get video metadata
            video_query = f"""
                SELECT video_id, title, channel_name, release_timestamp, duration
                FROM video_metadata
                WHERE video_id = '{video_id.replace(chr(39), chr(39)*2)}'
            """
            video_df = pd.read_sql_query(text(video_query), conn)

            if video_df.empty:
                return jsonify({'success': False, 'error': 'Video not found'}), 404

            video_info = video_df.iloc[0]

            # Get chat messages for this video
            chat_query = f"""
                SELECT
                    timestamp,
                    author_channel_id,
                    message,
                    video_offset_time_msec
                FROM live_chat
                WHERE video_id = '{video_id.replace(chr(39), chr(39)*2)}'
                ORDER BY timestamp
            """
            chat_df = pd.read_sql_query(text(chat_query), conn)

        if chat_df.empty:
            return jsonify({'success': False, 'error': 'No chat data found for this video'}), 404

        # Calculate statistics in time windows
        window_size_seconds = window_size_minutes * 60
        chat_df['time_window'] = (
            (chat_df['video_offset_time_msec'] / 1000) // window_size_seconds
        ).astype(int)

        # Group by time window
        stats = chat_df.groupby('time_window').agg({
            'author_channel_id': 'nunique',  # Unique chatters
            'message': 'count'  # Total messages
        }).reset_index()

        stats.columns = ['window', 'chatters', 'messages']

        # Calculate time labels
        stats['time_minutes'] = stats['window'] * window_size_minutes
        stats['time_label'] = stats['time_minutes'].apply(
            lambda x: f"{int(x//60):02d}:{int(x%60):02d}"
        )

        # Prepare data for Bokeh
        max_chatters = stats['chatters'].max()
        max_messages = stats['messages'].max()

        if max_messages > 0:
            stats['scaled_messages'] = stats['messages'] * (max_chatters / max_messages)
        else:
            stats['scaled_messages'] = 0

        # Create video URL
        stats['url'] = f"https://www.youtube.com/watch?v={video_id}&t={stats['time_minutes'] * 60}s"

        # Create Bokeh plot
        source = ColumnDataSource(data={
            'x': stats['window'].tolist(),
            'chatters': stats['chatters'].tolist(),
            'messages': stats['messages'].tolist(),
            'scaled_messages': stats['scaled_messages'].tolist(),
            'time_label': stats['time_label'].tolist(),
            'url': stats['url'].tolist()
        })

        # Create title with video info
        title = f"{video_info['title']}"
        if pd.notna(video_info['release_timestamp']):
            date_str = video_info['release_timestamp'].strftime('%Y-%m-%d')
            title += f" ({date_str})"

        p = figure(
            title=title,
            x_axis_label=f'Time ({window_size_minutes}-minute intervals)',
            y_axis_label='Unique Chatters',
            width=1000,
            height=400,
            toolbar_location='above',
            tools='pan,wheel_zoom,box_zoom,reset,save'
        )

        # Add bars for unique chatters
        bars = p.vbar(
            x='x',
            top='chatters',
            source=source,
            width=0.8,
            color='#1f77b4',
            legend_label='Unique Chatters',
            nonselection_fill_alpha=1.0,
            nonselection_fill_color='#1f77b4',
            nonselection_line_alpha=1.0
        )

        # Add line for messages
        line = p.line(
            'x',
            'scaled_messages',
            source=source,
            line_width=2,
            color='#FFD700',
            legend_label='Messages',
            nonselection_line_alpha=1.0
        )

        # Add circle markers
        circles = p.scatter(
            'x',
            'scaled_messages',
            source=source,
            size=6,
            color='#FFD700',
            marker='circle',
            nonselection_fill_alpha=1.0
        )

        # Add secondary y-axis for messages
        p.extra_y_ranges = {"messages": Range1d(start=0, end=max_messages * 1.1)}
        p.add_layout(LinearAxis(y_range_name="messages", axis_label="Messages"), 'right')

        # Add hover tool
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

        # Add tap tool for opening URLs
        tap_tool = TapTool()
        tap_tool.callback = CustomJS(args=dict(source=source), code="""
            const data = source.data;
            const index = source.selected.indices[0];
            if (index !== undefined) {
                const url = data['url'][index];
                window.open(url, '_blank');
                source.selected.indices = [];
            }
        """)
        p.add_tools(tap_tool)

        # Configure legend
        p.legend.location = "top_right"
        p.legend.click_policy = "hide"
        p.title.text_font_size = "12pt"

        # Generate Bokeh components for embedding
        script, div = components(p)

        return jsonify({
            'success': True,
            'video_id': video_id,
            'title': video_info['title'],
            'script': script,
            'div': div
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get general statistics about the database."""
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Get video count
            video_count_query = "SELECT COUNT(*) as count FROM video_metadata"
            video_count = pd.read_sql_query(text(video_count_query), conn).iloc[0]['count']

            # Get message count
            message_count_query = "SELECT COUNT(*) as count FROM live_chat"
            message_count = pd.read_sql_query(text(message_count_query), conn).iloc[0]['count']

            # Get date range
            date_range_query = """
                SELECT
                    MIN(release_timestamp) as earliest,
                    MAX(release_timestamp) as latest
                FROM video_metadata
            """
            date_range = pd.read_sql_query(text(date_range_query), conn).iloc[0]

        return jsonify({
            'success': True,
            'videos': int(video_count),
            'messages': int(message_count),
            'earliest_stream': date_range['earliest'].isoformat() if pd.notna(date_range['earliest']) else None,
            'latest_stream': date_range['latest'].isoformat() if pd.notna(date_range['latest']) else None
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

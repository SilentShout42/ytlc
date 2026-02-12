# YouTube Live Chat Stream Analyzer - Web Application

A web-based interface for searching and visualizing YouTube live chat streams from your database.

## Features

- 🔍 **Search Streams** - Search by title or date range
- 📊 **Interactive Plots** - Visualize chat activity with Bokeh plots showing:
  - Unique chatters over time
  - Message counts in time windows
  - Click on plot bars to jump to that timestamp in the video
- 📈 **Database Statistics** - View total streams, messages, and date ranges
- 🎨 **Modern UI** - Clean, responsive design that works on desktop and mobile

## Prerequisites

Make sure you have:
- PostgreSQL database running (via `docker compose up -d`)
- Environment variables set (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
- Python dependencies installed

## Installation

1. Install the required Python packages:
```bash
uv sync
```

Or if you prefer pip:
```bash
pip install flask bokeh pandas psycopg2
```

2. Make sure your database is running:
```bash
docker compose up -d
```

3. Load your environment variables:
```bash
source .envrc
```

## Running the Web App

From the project root directory, run:

```bash
python web/app.py
```

Or using the Makefile:

```bash
make web
```

The application will start on http://localhost:5000

## Usage

### Searching for Streams

1. **By Title**: Enter keywords in the "Title Search" field
2. **By Date**: Select a date range using the Start Date and End Date fields
3. **Combined Search**: Use both title and date filters together
4. **Adjust Results**: Change the "Max Results" to show more or fewer streams

### Viewing Plots

1. Click on any search result to generate and display its plot
2. The plot shows:
   - **Blue bars**: Unique chatters in each time window
   - **Yellow line**: Total messages (scaled to fit)
   - **Hover**: See exact numbers for each time window
   - **Click bars**: Opens YouTube at that timestamp (in new tab)
3. Adjust the "Plot Window" setting (in minutes) before searching to change the granularity

### Plot Features

- **Zoom**: Use mouse wheel or box zoom tool
- **Pan**: Click and drag to move around
- **Reset**: Reset to original view
- **Legend**: Click legend items to hide/show metrics
- **Timestamps**: Click any bar to open the stream at that point

## API Endpoints

The web app exposes the following REST API endpoints:

### GET /api/stats
Get database statistics (total videos, messages, date range)

### GET /api/search
Search for streams
- Parameters:
  - `title`: Search term for video title
  - `start_date`: Start date (YYYY-MM-DD)
  - `end_date`: End date (YYYY-MM-DD)
  - `limit`: Max results (default: 50)

### GET /api/plot/<video_id>
Generate plot for a specific video
- Parameters:
  - `window_size`: Time window in minutes (default: 5)

## File Structure

```
web/
├── app.py                 # Flask backend application
├── templates/
│   └── index.html        # Main HTML template
└── static/
    ├── css/
    │   └── style.css     # Styling
    └── js/
        └── app.js        # Frontend JavaScript
```

## Configuration

The app reads database configuration from environment variables:
- `PGHOST` - PostgreSQL host (default: localhost)
- `PGPORT` - PostgreSQL port (default: 5432)
- `PGDATABASE` - Database name (default: ytlc)
- `PGUSER` - Database user (default: ytlc)
- `PGPASSWORD` - Database password (required)

## Troubleshooting

**Connection refused**: Make sure the PostgreSQL container is running
```bash
docker compose ps
```

**No data showing**: Verify you have parsed data into the database
```bash
python main.py parse /path/to/data
```

**Plot not loading**: Check the browser console for JavaScript errors and ensure Bokeh CDN is accessible

**Environment variables not set**: Source the .envrc file
```bash
source .envrc
```

## Development

To run in development mode with auto-reload:

```bash
FLASK_ENV=development python web/app.py
```

## Future Enhancements

Potential features to add:
- Export search results to CSV
- Save favorite searches
- Compare multiple streams side-by-side
- Advanced filtering (by channel, duration, message count)
- User authentication for multi-user deployments
- Real-time updates when new data is parsed

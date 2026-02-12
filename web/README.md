# YTLC Web Application

A Flask-based web application for viewing YouTube live chat analytics with interactive Bokeh visualizations.

## Features

- **Video List**: Browse all videos with their titles and air dates
- **Interactive Charts**: Click any video to view detailed chat activity analysis
- **Bokeh Visualizations**: Interactive plots showing unique chatters over time with emoji overlays

## Setup

1. Install dependencies:
   ```bash
   pip install -e .
   ```

2. Ensure PostgreSQL database is running:
   ```bash
   docker-compose up -d
   ```

3. Run the Flask application:
   ```bash
   cd web
   python app.py
   ```

4. Open your browser and navigate to:
   ```
   http://localhost:5000
   ```

## Environment Variables

- `POSTGRES_USER`: Database user (default: ytlc)
- `POSTGRES_HOST`: Database host (default: localhost)
- `POSTGRES_PORT`: Database port (default: 5432)
- `POSTGRES_DB`: Database name (default: ytlc)

## Usage

1. **Home Page**: Displays a table of all videos. Click any row to view details.
2. **Video Detail Page**: Shows video information and an interactive Bokeh plot of chat activity over time.
   - The plot displays unique chatters in 5-minute windows
   - Hover over bars to see details
   - Top emojis for each window are displayed above the bars

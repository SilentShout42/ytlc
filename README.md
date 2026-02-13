# YouTube Live Chat (ytlc)

A tool for parsing and analyzing YouTube live chat data from archived streams.

## Features

- Parse YouTube live chat JSON files and video metadata
- Store chat messages in PostgreSQL database
- Search chat messages with regex patterns
- Track unique chatters and message counts in time windows

## Prerequisites

- Python 3.12+
- Docker and Docker Compose (for database)
- uv (Python package manager)

## Quick Start

### 1. Set up the database

Generate a random password and configure the database connection:

```bash
# Make the setup script executable and run it
chmod +x scripts/setup-db.sh
./scripts/setup-db.sh
```

This will:
- Generate a random password for the `ytlc` PostgreSQL user
- Create `.env` with the `POSTGRES_PASSWORD` environment variable (for Docker Compose)
- Create `.envrc` with PostgreSQL environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)

Now start the PostgreSQL database container:

```bash
docker compose up -d

# Check that the database is running
docker compose ps
```

The database will automatically:
- Create the `ytlc` database and
- Create the `ytlc` user with the generated password
- Initialize the database schema (tables and indexes)
- Persist data in a Docker volume

### 3. Load environment variables

You have two options to load the PostgreSQL connection variables from `.envrc`:

**Option A: Manual sourcing (recommended for simple workflows)**
```bash
source .envrc
# Now you can use: psql -h localhost -U ytlc -d ytlc
```

**Option B: Using direnv (recommended for automatic environment loading)**

If you have [direnv](https://direnv.net) installed:

```bash
# First time: allow direnv to load the environment
direnv allow

# Now the environment is automatically loaded when you cd into this directory
cd ..  # Leave the directory
cd ytlc  # Enter again - environment is automatically loaded
```

### 4. Install Python dependencies

```bash
uv sync
```

### 5. Parse YouTube data

Place your YouTube live chat JSON files in a directory, then parse them:

```bash
# Parse both video metadata (info.json) and live chat messages (live_chat.json)
uv run apps/cli/main.py parse /path/to/json/files
```

### 6. Analyze the data

```bash
# Search for messages matching a pattern
uv run apps/cli/main.py search "pattern1" "pattern2"
```

## Commands

### `parse`

Parse JSON files and load them into PostgreSQL.

```bash
uv run apps/cli/main.py parse DATA_DIR
```

Arguments:
- `DATA_DIR`: Directory containing both video info (info.json) and live chat (live_chat.json) JSON files

### `search`

Search messages and print results as markdown.

```bash
uv run apps/cli/main.py search REGEX_PATTERN [REGEX_PATTERN ...] [-o OUTPUT_FILE] [--debug]
```

Options:
- `REGEX_PATTERN`: One or more regex patterns to search for
- `-o, --output-file`: File to write results to
- `--debug`: Include Author and Message columns in output

### `missing_days`

Count missing video metadata days since 2024-05-25.

```bash
uv run apps/cli/main.py missing_days
```

## Web Application

The project includes a Flask-based web application for viewing chat analytics in a browser.

### Running the Web App

**Option 1: Using Make (recommended)**
```bash
make web
```

**Option 2: Manual**
```bash
# Make sure the database is running
docker compose up -d

# Start the Flask application
cd apps/web
uv run python app.py
```

Then open http://localhost:5000 in your browser.

### Web App Features

- **Video List**: Browse all videos with titles and air dates
- **Video Detail Pages**:
  - Embedded YouTube player
  - Interactive Bokeh charts showing chat activity
  - Click chart bars to jump to that timestamp in the video
  - Hover over bars for detailed stats
  - View unique chatters and message counts over time

See [apps/web/README.md](apps/web/README.md) for more details.

## Database Management

### Stop the database

```bash
docker compose down
```

### Reset the database

```bash
# Stop and remove the database container and volume
docker compose down -v

# Start fresh
docker compose up -d
```

### Access the database directly

```bash
# Using psql in the container
docker compose exec postgres psql -U ytlc -d ytlc

# Or from your host (if you have psql installed)
# First load the connection environment variables:
source .envrc
psql
```

The connection details are stored in `.envrc` as environment variables:
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

When you source `.envrc` (or use direnv), `psql` automatically uses these variables to connect. You can also override them individually:

```bash
source .envrc
psql -h localhost  # Uses PGHOST, but localhost overrides it
```

## Database Schema

### `video_metadata`

Stores metadata about YouTube videos.

- `video_id` (TEXT, PRIMARY KEY)
- `title` (TEXT)
- `channel_id` (TEXT)
- `channel_name` (TEXT)
- `release_timestamp` (TIMESTAMPTZ)
- `timestamp` (TIMESTAMPTZ)
- `duration` (INTERVAL)
- `was_live` (BOOLEAN)
- `filename` (TEXT)

### `live_chat`

Stores individual chat messages.

- `message_id` (TEXT, PRIMARY KEY)
- `timestamp` (TIMESTAMPTZ)
- `video_id` (TEXT)
- `author` (TEXT)
- `author_channel_id` (TEXT)
- `message` (TEXT)
- `is_moderator` (BOOLEAN)
- `is_channel_owner` (BOOLEAN)
- `video_offset_time_msec` (BIGINT)
- `video_offset_time_text` (TEXT)
- `filename` (TEXT)

## Configuration

The application connects to PostgreSQL with these defaults:

- Host: `localhost`
- Port: `5432`
- Database: `ytlc`
- User: `ytlc`
- Password: Auto-generated and stored in `.env` and `.envrc`

### Environment Files

The setup process creates two files in the project root:

- **`.env`**: Contains `POSTGRES_PASSWORD` - used by Docker Compose to set the database password
- **`.envrc`**: Contains PostgreSQL environment variables (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) for command-line tools like `psql`

Both files are auto-generated with a random password by `scripts/setup-db.sh` and are ignored by git for security.

To use the connection variables from `.envrc`:
- **Without direnv**: `source .envrc` before using psql
- **With direnv**: Run `direnv allow` once, then variables are auto-loaded when you cd into the directory

## Development

### Project Structure

```
ytlc/
├── apps/
│   ├── cli/             # CLI application
│   │   ├── main.py      # Main CLI entry point
│   │   └── parser/      # JSON parsing modules
│   │       ├── __init__.py
│   │       └── parser.py
│   └── web/             # Web application
│       ├── app.py       # Flask application
│       ├── templates/   # HTML templates
│       └── static/      # Static assets
├── scripts/             # Setup and utility scripts
│   └── setup-db.sh      # Database setup script (generates .env and .envrc)
├── misc/                # Miscellaneous files
│   ├── createdb.sh      # Legacy database setup script
│   └── init.sql         # Docker database initialization
├── saved_queries/       # SQL query examples
├── img/                 # Emoji images
├── docker-compose.yml   # Database container setup
├── .env.example         # Example environment variables
├── .envrc.example       # Example PostgreSQL connection variables
└── pyproject.toml       # Python dependencies
```

### Getting the `info.json` and `live_chat.json` dataset

Options explanation:
* `--rate-limit 10M` adds rate limiting to 10 MB/s
* `--no-download` skips download of audio/video
* `--no-wait --no-ignore-no-formats-error` stops yt-dlp from waiting for an upcoming live stream or erroring out on a pre-live video.
* `--no-overwrite` prevents yt-dlp from overwriting existing live chat or video metadata files. This way you can re-run these commands as-is without re-downloading content. Consider using the `--download-archive` option if you want to save even more time for repeated runs (at the cost of some statefulness.)

Get a list of all vods for use in subsequent commands - adjust the URL to match the channel you're working with

```shell
yt-dlp \
  --dump-json \
  --flat-playlist \
  'https://www.youtube.com/@KannaYanagi/streams' | \
  jq -r 'select(.was_live==true) | .url' | \
  tee all.txt
```

Fetch live chat transcripts (live_chat.json)
```shell
yt-dlp \
  -t sleep \
  --rate-lmit 10M \
  --no-download \
  --no-wait \
  --no-ignore-no-formats-error \
  --write-subs \
  --sub-langs live_chat \
  --no-overwrite \
  --batch-file all.txt
```

Fetch video metadata (info.json)
```shell
yt-dlp \
  -t sleep \
  --rate-lmit 10M \
  --no-download \
  --no-wait \
  --no-ignore-no-formats-error \
  --write-info-json \
  --no-overwrite \
  --batch-file all.txt
```

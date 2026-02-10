# YouTube Live Chat (ytlc)

A tool for parsing and analyzing YouTube live chat data from archived streams.

## Features

- Parse YouTube live chat JSON files and video metadata
- Store chat messages in PostgreSQL database
- Search chat messages with regex patterns
- Generate interactive visualizations of chat activity over time
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
- Create the `ytlc` database
- Create the `ytlc` user with the generated password
- Initialize the database schema (tables and indexes)
- Persist data in a Docker volume

### 2. Load environment variables

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

### 2. Install Python dependencies

```bash
uv sync
```

### 3. Parse YouTube data

Place your YouTube live chat JSON files in a directory, then parse them:

```bash
# Parse video metadata (info.json files)
uv run main.py parse --info-json /path/to/info/json/files

# Parse live chat messages (live_chat.json files)
uv run main.py parse --live-chat-json /path/to/chat/json/files
```

### 4. Analyze the data

```bash
# Search for messages matching a pattern
uv run main.py search "pattern1" "pattern2"

# Generate interactive plots of chat activity
uv run main.py plot --last-n 5 -o output.html

# Plot specific videos by ID
uv run main.py plot VIDEO_ID1 VIDEO_ID2 -o output.html

# Plot videos within a date range
uv run main.py plot --start-date 2026-01-01 --end-date 2026-01-31 -o output.html
```

## Commands

### `parse`

Parse JSON files and load them into PostgreSQL.

```bash
uv run main.py parse --info-json DIR --live-chat-json DIR
```

Options:
- `--info-json DIR`: Directory containing video info JSON files
- `--live-chat-json DIR`: Directory containing live chat JSON files

### `search`

Search messages and print results as markdown.

```bash
uv run main.py search REGEX_PATTERN [REGEX_PATTERN ...] [-o OUTPUT_FILE] [--debug]
```

Options:
- `REGEX_PATTERN`: One or more regex patterns to search for
- `-o, --output-file`: File to write results to
- `--debug`: Include Author and Message columns in output

### `plot`

Create interactive histograms of unique chatters over stream duration.

```bash
uv run main.py plot [VIDEO_ID ...] [-w MINUTES] [-o OUTPUT_FILE] [--last-n COUNT] [--start-date DATE] [--end-date DATE]
```

Options:
- `VIDEO_ID`: Specific video IDs to plot (optional)
- `-w, --window-size MINUTES`: Time window size in minutes (default: 5)
- `-o, --output-file`: File to save the plot (HTML format)
- `--last-n COUNT`: Plot the most recent N VODs
- `--start-date YYYY-MM-DD`: Filter VODs from this date (inclusive)
- `--end-date YYYY-MM-DD`: Filter VODs up to this date (inclusive)

### `missing_days`

Count missing video metadata days since 2024-05-25.

```bash
uv run main.py missing_days
```

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
├── main.py              # Main CLI application
├── parser/              # JSON parsing modules
│   ├── __init__.py
│   └── parser.py
├── scripts/             # Setup and utility scripts
│   └── setup-db.sh      # Database setup script (generates .env and .envrc)
├── misc/                # Miscellaneous files
│   ├── createdb.sh      # Legacy database setup script
│   └── init.sql         # Docker database initialization
├── saved_queries/       # SQL query examples
├── docker-compose.yml   # Database container setup
├── .env.example         # Example environment variables
├── .envrc.example       # Example PostgreSQL connection variables
└── pyproject.toml       # Python dependencies
```

### Legacy Database Setup

If you prefer to run PostgreSQL natively instead of using Docker, you can use the legacy setup script:

```bash
./misc/createdb.sh
```

This will create the database and user on your local PostgreSQL installation.

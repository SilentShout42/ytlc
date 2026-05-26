# YouTube Live Chat (ytlc)

A tool for parsing and analyzing YouTube live chat data from archived streams.

**Rust-only.** Uses SQLite for storage — no external database required.

## Prerequisites

- Rust toolchain (cargo)

## Quick Start

### 1. Build the binary

```bash
make release
# or: cargo build --release
```

### 2. Set the database path (optional)

By default, data is stored in `ytlc.db` in the current directory. Override with the `YTLC_DB` env var:

```bash
export YTLC_DB="/path/to/ytlc.db"
```

### 3. Parse YouTube data

Place your YouTube live chat JSON files in a directory (files from [yt-dlp](https://github.com/yt-dlp/yt-dlp)), then parse them:

```bash
# Parse video metadata (info.json files)
./target/release/ytlc parse DATA_DIR info

# Parse live chat messages (live_chat.json files)
./target/release/ytlc parse DATA_DIR live_chat
```

### 4. Analyze the data

```bash
# Search for messages matching a pattern
./target/release/ytlc search "regex_pattern1" "regex_pattern2"

# Search with output to a file
./target/release/ytlc search "pattern" -o results.md

# Debug mode (includes Author and Message columns)
./target/release/ytlc search "pattern" --debug

# Check database health
./target/release/ytlc dbcheck
```

## Commands

### `parse`

Parse JSON files and load them into SQLite.

```bash
./target/release/ytlc parse DATA_DIR TYPE
```

Arguments:
- `DATA_DIR`: Directory containing video info and live chat JSON files
- `TYPE`: Either `info` (video metadata) or `live_chat` (chat messages)

### `search`

Search messages and print results as a markdown table with windowed results (≥5 matches per 60-second window per video).

```bash
./target/release/ytlc search REGEX_PATTERN [REGEX_PATTERN ...] [-o OUTPUT_FILE] [--debug]
```

Options:
- `REGEX_PATTERN`: One or more regex patterns to search for (case-insensitive)
- `-o, --output-file`: File to write results to
- `--debug`: Include Author and Message columns in output

### `dbcheck`

Test the database connection and show row counts.

```bash
./target/release/ytlc dbcheck
```

## Database Schema

SQLite is used automatically on first access — no setup required.

### `video_metadata`

Stores metadata about YouTube videos.

| Column | Type |
|--------|------|
| `video_id` | TEXT, PRIMARY KEY |
| `title` | TEXT |
| `channel_id` | TEXT |
| `channel_name` | TEXT |
| `release_timestamp` | TEXT |
| `timestamp` | TEXT |
| `duration` | BIGINT |
| `was_live` | INTEGER |
| `filename` | TEXT |

### `live_chat`

Stores individual chat messages.

| Column | Type |
|--------|------|
| `message_id` | TEXT, PRIMARY KEY |
| `timestamp` | TEXT |
| `video_id` | TEXT |
| `author` | TEXT |
| `author_channel_id` | TEXT |
| `message` | TEXT |
| `is_moderator` | INTEGER |
| `is_channel_owner` | INTEGER |
| `video_offset_time_msec` | BIGINT |
| `video_offset_time_text` | TEXT |
| `filename` | TEXT |

## Getting the `info.json` and `live_chat.json` dataset

Options explanation:
- `--rate-limit 10M` adds rate limiting to 10 MB/s
- `--no-download` skips download of audio/video
- `--no-wait --no-ignore-no-formats-error` stops yt-dlp from waiting for an upcoming live stream or erroring out on a pre-live video.
- `--no-overwrite` prevents yt-dlp from overwriting existing live chat or video metadata files. This way you can re-run these commands as-is without re-downloading content. Consider using the `--download-archive` option if you want to save even more time for repeated runs (at the cost of some statefulness.)

Get a list of all VODs for use in subsequent commands — adjust the URL to match the channel:

```bash
yt-dlp \
  --dump-json \
  --flat-playlist \
  'https://www.youtube.com/@KannaYanagi/streams' | \
  jq -r 'select(.was_live==true) | .url' | \
  tee all.txt
```

Fetch live chat transcripts (`live_chat.json`):

```bash
yt-dlp \
  -t sleep \
  --rate-limit 10M \
  --no-download \
  --no-wait \
  --no-ignore-no-formats-error \
  --write-subs \
  --sub-langs live_chat \
  --no-overwrite \
  --batch-file all.txt
```

Fetch video metadata (`info.json`):

```bash
yt-dlp \
  -t sleep \
  --rate-limit 10M \
  --no-download \
  --no-wait \
  --no-ignore-no-formats-error \
  --write-info-json \
  --no-overwrite \
  --batch-file all.txt
```

## Project Structure

```
ytlc/
├── main.rs            # CLI entry point
├── parser.rs          # JSON file parsing (info & live_chat)
├── search.rs          # Regex search with windowing
├── Cargo.toml         # Rust dependencies
├── misc/              # Reference SQL schema
│   └── init.sql       # SQLite schema reference (not executed by the app)
├── scripts/           # Utility scripts
├── saved_queries/     # SQL query examples
└── Makefile           # Build targets (build, release, clean)
```

## Make Targets

| Target | Description |
|--------|-------------|
| `make build` | Build debug binary |
| `make release` | Build optimized release binary |
| `make clean` | Remove build artifacts |
| `make help` | Show available targets |

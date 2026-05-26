-- SQLite database schema for ytlc
-- SQLite does not require a separate initialization step — the database
-- file is created automatically on first connect. This file is provided
-- as a reference for the schema and is NOT run by the application.

-- Video metadata table
CREATE TABLE IF NOT EXISTS video_metadata (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel_id TEXT,
    channel_name TEXT,
    release_timestamp TEXT,
    timestamp TEXT,
    duration BIGINT,
    was_live INTEGER,
    filename TEXT
);

-- Live chat messages table
CREATE TABLE IF NOT EXISTS live_chat (
    message_id TEXT PRIMARY KEY,
    timestamp TEXT,
    video_id TEXT,
    author TEXT,
    author_channel_id TEXT,
    message TEXT,
    is_moderator INTEGER,
    is_channel_owner INTEGER,
    video_offset_time_msec BIGINT,
    video_offset_time_text TEXT,
    filename TEXT
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_live_chat_video_id ON live_chat(video_id);
CREATE INDEX IF NOT EXISTS idx_live_chat_timestamp ON live_chat(timestamp);
CREATE INDEX IF NOT EXISTS idx_live_chat_author_channel_id ON live_chat(author_channel_id);
CREATE INDEX IF NOT EXISTS idx_live_chat_author ON live_chat(author);
CREATE INDEX IF NOT EXISTS idx_video_metadata_release_timestamp ON video_metadata(release_timestamp);

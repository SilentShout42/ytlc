-- Initialize ytlc database schema
-- This script runs automatically when the PostgreSQL container first starts

-- Set timezone to UTC
SET TIME ZONE 'UTC';

-- Create video_metadata table
CREATE TABLE IF NOT EXISTS video_metadata (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel_id TEXT,
    channel_name TEXT,
    release_timestamp TIMESTAMPTZ,
    timestamp TIMESTAMPTZ,
    duration INTERVAL,
    was_live BOOLEAN,
    filename TEXT
);

-- Create live_chat table
CREATE TABLE IF NOT EXISTS live_chat (
    message_id TEXT PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    video_id TEXT,
    author TEXT,
    author_channel_id TEXT,
    message TEXT,
    is_moderator BOOLEAN,
    is_channel_owner BOOLEAN,
    video_offset_time_msec BIGINT,
    video_offset_time_text TEXT,
    filename TEXT
);

-- Enable extensions for advanced indexing
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create standard B-tree indexes for exact matches and sorting
CREATE INDEX IF NOT EXISTS idx_live_chat_video_id ON live_chat(video_id);
CREATE INDEX IF NOT EXISTS idx_live_chat_timestamp ON live_chat(timestamp);
CREATE INDEX IF NOT EXISTS idx_live_chat_author_channel_id ON live_chat(author_channel_id);
CREATE INDEX IF NOT EXISTS idx_live_chat_author ON live_chat(author);
CREATE INDEX IF NOT EXISTS idx_video_metadata_release_timestamp ON video_metadata(release_timestamp);

-- Create GIN trigram index for fast regex and LIKE pattern matching on messages
-- This enables efficient searching with ~, ~~, and ~* operators
CREATE INDEX IF NOT EXISTS idx_live_chat_message_trgm ON live_chat USING GIN(message gin_trgm_ops);

-- Create full-text search index for keyword-based searches
-- Converts message to tsvector for efficient keyword matching with @@ operator
CREATE INDEX IF NOT EXISTS idx_live_chat_message_fts ON live_chat USING GIN(to_tsvector('english', message));

-- Grant necessary privileges (already set by POSTGRES_USER env var, but explicit for clarity)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ytlc;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ytlc;

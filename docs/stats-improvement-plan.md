# Plan: Improving Notable Moment Detection in ytlc Stats

## 1. Understand

**Goal:** Improve the `stats` subcommand (`ytlc stats`) to better detect "notable moments" in YouTube live chat streams by moving beyond the current simple approach of ranking chunks by raw unique author count per fixed-size time window. The user specifically asks about:

- Computing a **rolling average of unique chatter** and picking times ~2 minutes before peaks
- Exploring **other signal improvements** for chat activity analysis

**Current implementation** (`src/stats.rs`): Chunks the video into fixed-size windows (default 5 min), counts unique `author_channel_id` per chunk, and ranks chunks by that count. Also prints aggregate summary stats.

**Assumptions:**
- The tool runs on the SQLite DB produced by `ytlc parse`; no DB schema changes are required (all computation at query time)
- We want to produce a **subcommand enhancement**, not a standalone new tool
- "Notable" should capture chat energy/hype, not just volume — so we should consider message rate, unique chatters, and surprise (deviation from baseline)
- The user's "2 minutes before peak" idea suggests we want to **predict or locate the ramp-up phase** of chat activity, not just the peak itself (since the peak is the climax, the ramp-up is where the clip-worthy content usually starts)
- No message content analysis (emoji, spam ratio, etc.) for now
- No schema changes — all computation is query-time
- No author-name-based deduplication option; `author_channel_id` is stable and sufficient

---

## 2. Explore

### Files that will be touched

| File | Why |
|------|-----|
| `src/stats.rs` | **Primary.** Add new ranking strategies, rolling window math, CLI output enhancements, member filtering |
| `src/parser.rs` | Add `is_member` field to `ChatMessage`, extract MEMBER badge, add column to schema and inserts |
| `src/main.rs` | Add new CLI flags to `Stats` subcommand (`--rank-by`, `--lookback`, `--rolling-window`, `--members-only`) |

### Files NOT touched
- `src/parser.rs` — schema is adequate
- `src/search.rs` — unrelated
- `Cargo.toml` — all needed crates already present
- DB schema — no changes

### Current stats.rs analysis

- **Chunking loop**: Iterates `0..total_secs` in `chunk_duration` steps, fetching rows per-chunk via SQL with `>=`/`<` filter. Each chunk allocates a `HashSet<String>` for unique authors.
- **Performance concern**: Per-chunk queries are O(n_chunks × rows_per_chunk) — each chunk is a separate DB round-trip. Refactoring to a single query + in-memory chunking is the clear optimization path.

---

## 3. Design

### High-level approach

Introduce a **modular ranking strategy** system in the stats subcommand, with multiple algorithms and a CLI flag to select among them.

### Strategies to implement

| # | Strategy | Description |
|---|----------|-------------|
| A | `z-score-unique` **(default)** | Z-score of unique authors relative to stream mean/stddev. Highlights anomalies — chunks that are unusual compared to the stream's normal chatter level. Best for long streams where early chunks may have more unique chatters simply due to more elapsed time. |
| B | `unique-authors` | Raw unique author count per chunk | Baseline, backward compatible |
| C | `message-rate` | Total messages per chunk (per second) | Captures spam/flood moments that unique count underweights |
| D | `rolling-peak` | Compute a trailing rolling average of unique authors (default window: 3 chunks), find peak, report the chunk ~lookback seconds before it | Captures the "hype build-up" phase — trailing (not centered) to avoid future-data leakage |
| E | `--members-only` flag | Filter to only channel member messages when ranking | For analyzing member chat engagement specifically |

### `--members-only` flag — detailed design

YouTube live chat JSON includes `authorBadges` with `iconType` values like "MODERATOR", "OWNER", and "MEMBER". This flag:
1. Adds `is_member` column to `live_chat` table (BOOLEAN NOT NULL DEFAULT 0)
2. Parser extracts member status from author badges — member badges use `customThumbnail` (no `iconType`) and `tooltip` containing "Member" (e.g. "Member (1 year)")
3. Stats queries add `AND is_member = 1` filter to all member-limited queries
4. Output shows "— members only" label in the header
5. Existing data gets column added with default 0; new parses will capture member badges automatically

### User's rolling-average-before-peak — detailed design

1. Compute unique authors per chunk (as current)
2. Apply a **rolling average** window (configurable, default 3 chunks) to smooth noise
3. Find the **peak** of the smoothed series
4. Walk backward from the peak and select the chunk whose start time falls within `lookback` seconds (default: configurable) before it
5. Return as a notable moment with context: the peak it precedes and the peak's height
6. Clamp lookback to valid range (don't go before 0 or after stream end)

This is powerful because chat activity typically **builds up** before something happens — the peak is usually the reveal/climax/reaction, and the time before is where people are setting up, guessing, getting excited. That's the narrative arc.

### Optimized query approach

Instead of per-chunk loops, fetch all data in one query and chunk in Rust:

```sql
SELECT author_channel_id,
       video_offset_time_msec
FROM live_chat
WHERE video_id = ?
  AND video_offset_time_msec IS NOT NULL
ORDER BY video_offset_time_msec;
```

Stream rows lazily via `query_map` iterator; don't collect into Vec. Iterate once, bucket into chunks, aggregate — O(n) single-pass instead of O(n_chunks × rows_per_chunk).

### Design decisions / alternatives rejected

| Decision | Alternative | Reasoning |
|----------|-------------|-----------|
| Single SQL query + in-memory chunking | Per-chunk SQL query with separate HashSet | Clear O(n) improvement; lazy streaming avoids OOM on large streams |
| Rust-side computation vs. SQL-side | Compute in Rust | More flexible for z-score, rolling avg, lookback — awkward in SQL |
| Add new subcommand vs. extending existing | New `ytlc find-hype` subcommand | Extending is cleaner; users already know `stats`. Flags, not new commands. |
| Fixed chunk size for all strategies | Adaptive chunking (e.g., per 100 messages) | Fixed chunk size aligns with timestamps. Adaptive is interesting but harder to explain. Keep fixed for v1. |
| Z-score as default | Keep unique-authors as default | Z-score is a meaningful improvement — anomaly detection beats raw counts for real streams |

---

## 4. Steps

### Step 1: Refactor data fetching — single query + in-memory chunking
- Replace the per-chunk SQL loop with a single query fetching `(author_channel_id, video_offset_time_msec)` ordered by offset
- In Rust, iterate the result set once via lazy iterator, bucket into chunks, compute per-chunk stats (unique authors, total messages)
- **Verification**: Run existing `ytlc stats <vid>` — output should be identical for `--rank-by unique-authors`

### Step 2: Add z-score strategy (new default)
- Compute mean and stddev of unique authors across all chunks
- For each chunk, compute z = (x − μ) / σ
- Rank by z-score descending
- Handle edge case: σ < 1.0 → fall back to raw unique-authors ranking (no meaningful variance)
- **Verification**: On a uniform stream, z-scores should all be near 0; on a spike stream, spikes should have high z

### Step 3: Add rolling-peak strategy
- Implement trailing rolling average (configurable window in chunks, default 3) — trailing (not centered) to avoid future-data leakage
- Find peak of smoothed series
- Walk backward by `lookback` seconds (configurable, default 120) from peak
- Mark as notable moment with metadata: lookback time, peak time, peak unique count
- Clamp lookback to valid range (start ≥ 0)
- **Verification**: Run on a test stream; verify reported time is ~lookback seconds before the actual chat peak

### Step 4: Add message-rate strategy
- Count total messages per chunk, compute rate per second
- Simple ranking, included for completeness
- **Verification**: Should highlight spam/flood moments that unique-authors misses

### Step 5: Add CLI flags
- `--rank-by <strategy>` — select ranking strategy: `z-score-unique`, `unique-authors`, `message-rate`, `rolling-peak` (default: `z-score-unique`)
- `--lookback <seconds>` — for `rolling-peak` strategy, how far back from peak to report (default: 120)
- `--rolling-window <chunks>` — rolling average window size for `rolling-peak` (default: 3)
- `--members-only` — filter to only channel member messages when ranking
- **Verification**: `ytlc stats --help` shows new flags; each flag produces expected output

### Step 6: Output enhancement
- For `rolling-peak` output, show **both** the lookback time and the peak time
- Include columns: `Time`, `Peak At (time)`, `Peak Uniques`, `Lookback`
- For z-score output, include the z-score value in the table
- Append "— members only" label when `--members-only` is set
- **Verification**: Markdown output is readable and informative

---

## 5. Risks & Edge Cases

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Lazy iterator lifetime** — rusqlite rows borrow the connection | Compile errors if conn dropped too early | Keep conn alive until all chunking is done; collect chunks into Vec before any further processing |
| **Very long streams** (10k+ messages) | Memory from chunk Vec | Vec<ChunkStats> is small (~8 bytes per chunk, so 1000 chunks ≈ 8 KB). Fine. |
| **Rolling average peak at start/end** of stream | Lookback goes before 0 or after end | Clamp lookback to valid range; if peak is too close to start, report closest valid chunk |
| **σ < 1.0** in z-score | Division by zero or near-zero → extreme values | If σ < 1.0, fall back to raw unique-authors ranking |
| **Backward compatibility** | Users relying on current `ytlc stats` behavior | Default `--rank-by` changed to `z-score-unique`, but output format (markdown table) is identical — safe migration |
| **Very short videos** (< chunk size) | Only 1 chunk → all strategies degenerate | Report a single "all activity" row with a note |
| **Missing `video_offset_time_msec`** | Some messages may lack offset | Already filtered out by `IS NOT NULL` in current code |
| **Chunk boundary effects** | A real spike split across two chunks may appear weaker | Mention chunk size in output; acceptable trade-off |

---

## 6. Open Questions (resolved)

| Question | Resolution |
|----------|------------|
| 1. Default strategy? | Changed to `z-score-unique` — anomaly detection beats raw counts |
| 2. Lookback value? | Configurable via `--lookback`, default 120 seconds |
| 3. Rolling window size? | Configurable via `--rolling-window`, default 3 chunks |
| 4. Show both times? | Yes — lookback time and peak time both displayed |
| 5. Message content signals? | Skipped for now (emoji, spam ratio, author repeat rate) |
| 6. DB schema changes? | No — all computation is query-time |
| 7. Author-name dedup option? | Not offered — `author_channel_id` is stable and sufficient |

---

## Files Changed Summary

| File | Changes |
|------|---------|
| `src/stats.rs` | Refactor to single-query fetch; implement z-score, rolling-peak, message-rate strategies; enhance output; add member filtering |
| `src/parser.rs` | Add `is_member` field, extract MEMBER badge, add column to schema and inserts |
| `src/main.rs` | Add `--rank-by`, `--lookback`, `--rolling-window`, `--members-only` flags to `Stats` subcommand |

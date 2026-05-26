#!/usr/bin/env bash
set -euo pipefail

# ytlc now uses SQLite (a file-based database).
# No database creation step is needed — the .db file is created automatically
# on first connect. To use a custom path, set the YTLC_DB environment variable:
#
#   export YTLC_DB="/path/to/ytlc.db"
#   ./ytlc parse <data_dir>

echo "ytlc uses SQLite. Database file: ${YTLC_DB:-ytlc.db}"
echo "No database creation step is required."

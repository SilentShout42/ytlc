#!/bin/bash
# Generate random password and create .envrc for local development

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
ENVRC_FILE="$PROJECT_ROOT/.envrc"

# Generate a random password if .env doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo "Generating random password for PostgreSQL..."
    RANDOM_PASSWORD=$(openssl rand -base64 16 | tr -d '\n')
    echo "POSTGRES_PASSWORD=$RANDOM_PASSWORD" > "$ENV_FILE"
    echo "✓ Created $ENV_FILE with generated password"
else
    # Extract password from existing .env (handles passwords containing '=')
    RANDOM_PASSWORD=$(grep '^POSTGRES_PASSWORD=' "$ENV_FILE" | sed 's/^POSTGRES_PASSWORD=//')
fi

# Create .envrc file with psql connection environment variables
echo "Creating $ENVRC_FILE..."
cat > "$ENVRC_FILE" << EOF
# PostgreSQL connection environment variables
# Source this file with: source .envrc
# Or use with direnv: install direnv, then 'direnv allow'

export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=ytlc
export PGUSER=ytlc
export PGPASSWORD=$RANDOM_PASSWORD
EOF
chmod 600 "$ENVRC_FILE"
echo "✓ Created $ENVRC_FILE with database connection variables"

echo ""
echo "Database setup complete!"
echo "- PostgreSQL password: $RANDOM_PASSWORD"
echo "- Environment file: $ENV_FILE"
echo "- Connection variables file: $ENVRC_FILE"
echo ""
echo "To use the connection variables:"
echo "  Option 1: source .envrc"
echo "  Option 2: install direnv and run 'direnv allow' (auto-loads on cd)"
echo ""
echo "Start the database with: docker compose up -d"

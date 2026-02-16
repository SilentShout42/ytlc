.PHONY: init destroy db-up db-down db-reset db-dump db-restore clean web web-gunicorn help

help:
	@echo "ytlc - YouTube Live Chat Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  init              First-time setup (database + Python deps)"
	@echo "  destroy           Reset everything (database + env files)"
	@echo "  db-up             Start PostgreSQL database container"
	@echo "  db-down           Stop PostgreSQL database container"
	@echo "  db-reset          Stop database and remove volume"
	@echo "  db-dump           Backup database to backups/ytlc-TIMESTAMP.sql"
	@echo "  db-restore FILE=<path>  Restore database from backup file"
	@echo "  web               Start the Flask web application"
	@echo "  web-gunicorn      Start Flask with gunicorn (production)"
	@echo "  clean             Remove Python cache files and __pycache__"
	@echo "  help              Show this help message"
	@echo ""

init: scripts/setup-db.sh
	@echo "🚀 Running first-time setup..."
	@chmod +x scripts/setup-db.sh
	@./scripts/setup-db.sh
	@echo "✅ Database credentials generated (.env and .envrc)"
	@echo ""
	@echo "📦 Starting PostgreSQL database..."
	@docker compose up -d
	@docker compose ps
	@echo "✅ Database started"
	@echo ""
	@echo "📚 Installing Python dependencies..."
	@uv sync
	@echo "✅ Dependencies installed"
	@echo ""
	@echo "🎉 Setup complete!"
	@echo "   Next: source .envrc (or use direnv allow)"

destroy:
	@echo "💥 Destroying all resources..."
	@echo ""
	@echo "⏹️  Stopping and removing database container + volume..."
	@docker compose down -v || true
	@echo "✅ Database removed"
	@echo ""
	@echo "🗑️  Removing generated environment files..."
	@rm -f .env .envrc
	@rm -f .python-version
	@echo "✅ Environment files removed"
	@echo ""
	@echo "🧹 Cleaning Python cache..."
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete || true
	@echo "✅ Cache cleaned"
	@echo ""
	@echo "⚠️  To completely reset, you may also want to delete:"
	@echo "   - .venv/ (if local virtual environment)"
	@echo "   - .direnv/ (if using direnv)"

db-up:
	@echo "📦 Starting PostgreSQL database..."
	@docker compose up -d
	@docker compose ps

db-down:
	@echo "⏹️  Stopping PostgreSQL database..."
	@docker compose down
	@echo "✅ Database stopped"

db-reset: db-down
	@echo "💥 Removing database volume..."
	@docker volume rm ytlc_postgres_data 2>/dev/null || true
	@echo "✅ Volume removed"
	@echo "🚀 Run 'make init' to recreate the database"

db-dump:
	@echo "💾 Creating database backup..."
	@mkdir -p backups
	@if [ ! -f .env ]; then echo "❌ Database not initialized. Run 'make init' first."; exit 1; fi
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	docker compose exec -T postgres pg_dump -U ytlc ytlc > backups/ytlc-$$TIMESTAMP.sql; \
	echo "✅ Database backed up to backups/ytlc-$$TIMESTAMP.sql"

db-restore:
	@echo "📥 Restoring database from backup..."
	@if [ ! -f .env ]; then echo "❌ Database not initialized. Run 'make init' first."; exit 1; fi
	@if [ -z "$(FILE)" ]; then \
		echo "❌ Please specify a backup file: make db-restore FILE=backups/ytlc-20260210_120000.sql"; \
		echo "📋 Available backups:"; \
		ls -lh backups/ytlc-*.sql 2>/dev/null || echo "   (none found)"; \
		exit 1; \
	fi
	@if [ ! -f "$(FILE)" ]; then echo "❌ File not found: $(FILE)"; exit 1; fi
	@echo "⏳ This will overwrite the current database. Proceeding..."
	@docker compose exec -T postgres psql -U ytlc ytlc < $(FILE)
	@echo "✅ Database restored from $(FILE)"

clean:
	@echo "🧹 Cleaning Python cache..."
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete || true
	@find . -type f -name ".coverage*" -delete || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cache cleaned"

web:
	@echo "🌐 Starting Flask web application..."
	@if [ ! -f .env ]; then echo "❌ Database not initialized. Run 'make init' first."; exit 1; fi
	@echo "📍 Web app will be available at: http://localhost:5000"
	@echo "🛑 Press Ctrl+C to stop the server"
	@cd apps/web && uv run python app.py

web-gunicorn:
	@echo "🌐 Starting Flask with gunicorn..."
	@if [ ! -f .env ]; then echo "❌ Database not initialized. Run 'make init' first."; exit 1; fi
	@echo "📍 Web app will be available at: http://localhost:8000"
	@echo "🛑 Press Ctrl+C to stop the server"
	@cd apps/web && uv run gunicorn --reload -b 127.0.0.1:8000 -w 4 --timeout 120 app:app

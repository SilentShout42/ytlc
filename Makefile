.PHONY: build release clean help

help:
	@echo "ytlc - YouTube Live Chat Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  build     Build the Rust binary (debug)"
	@echo "  release   Build the Rust binary (optimized)"
	@echo "  clean     Remove build artifacts and Python cache"
	@echo "  help      Show this help message"
	@echo ""

build:
	cargo build

release:
	cargo build --release

clean:
	@echo "Cleaning build artifacts and Python cache..."
	@cargo clean 2>/dev/null || true
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete || true
	@echo "Done"

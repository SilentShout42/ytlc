.PHONY: build release clean help

help:
	@echo "ytlc - YouTube Live Chat Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  build     Build the Rust binary (debug)"
	@echo "  release   Build the Rust binary (optimized)"
	@echo "  clean     Remove build artifacts"
	@echo "  help      Show this help message"
	@echo ""

build:
	cargo build

release:
	cargo build --release

clean:
	@echo "Cleaning build artifacts..."
	@cargo clean 2>/dev/null || true
	@echo "Done"

.PHONY: deps run run-simd

deps:
	cargo fetch

gen-input:
	cd data && python3 create_measurements.py 1_000_000_000

run:
	cargo run --release

run-simd:
	cargo +nightly run --release --features "simd-index"

just-run:
	@echo "Generating input data..."
	make gen-input
	@echo "Fetching deps"
	make deps
	@echo "Running the project..."
	make run
.PHONY: doc all release shell run1 run2 run3

all:
	clear; cargo build --all-features

doc:
	cd doc/book; mdbook build

release:
	nix-build --arg release true

shell:
	nix-shell

run1:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config.1.toml server

run2:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config.2.toml server

run3:
	RUST_LOG=garage=debug ./target/debug/garage -c tmp/config.3.toml server

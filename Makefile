#
# Copyright 2020 Joyent, Inc
#

#
# Variables
#

NAME = tokio-zookeeper
CARGO ?= cargo
RUST_CLIPPY_ARGS ?= -- -D clippy::all
RUSTFMT_ARGS ?= -- --check

#
# Repo-specific targets
#
.PHONY: all
all: build-tokio-zookeeper

.PHONY: build-tokio-zookeeper
build-tokio-zookeeper:
	$(CARGO) build --release

.PHONY: test
test:
	$(CARGO) test

.PHONY: test-unit
test-unit:
	$(CARGO) test --lib

.PHONY: check
check:
	$(CARGO) clean && $(CARGO) clippy $(RUST_CLIPPY_ARGS)

.PHONY: fmtcheck
fmtcheck:
	$(CARGO) fmt $(RUSTFMT_ARGS)

#!/usr/bin/env bash
# The CI checks, run locally: formatting, clippy (warnings are errors) and the
# tests, doctests included.
#
#   scripts/check.sh
#
# Builds without a rustc wrapper: sccache has produced corrupt proc-macro
# dylibs in release builds.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export RUSTC_WRAPPER=

step() { printf '\n== %s\n' "$*"; }

step "fmt"
cargo fmt --all --check

step "clippy"
cargo clippy --all-targets --locked -- -D warnings

step "tests"
cargo test --locked

step "all checks passed"

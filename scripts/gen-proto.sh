#!/usr/bin/env bash
# Regenerate src/proto/ from proto/osmformat.proto. Needs `protoc` on PATH.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
export RUSTC_WRAPPER=
rm -rf "$root/src/proto"
cargo run --quiet --release --manifest-path "$root/tools/gen-proto/Cargo.toml" -- "$root"
echo "regenerated $root/src/proto"

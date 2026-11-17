//! Generates the crate's buffa code from `proto/osmformat.proto`.
//!
//! Usage: `gen-proto <crate root>`. Writes `src/proto/`.

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("usage: gen-proto <crate root>"));
    let out = root.join("src/proto");
    std::fs::create_dir_all(&out).expect("create src/proto");
    buffa_build::Config::new()
        .files(&[root.join("proto/osmformat.proto")])
        .includes(&[root.join("proto")])
        .generate_views(true)
        // Fields the schema leaves out are skipped, not kept as unknown.
        .preserve_unknown_fields(false)
        .out_dir(out)
        .include_file("mod.rs")
        .compile()
        .expect("generate buffa code");
}

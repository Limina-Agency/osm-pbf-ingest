# Contributing

Issues and pull requests are welcome.

- `scripts/check.sh` passes (it treats clippy warnings as errors).
- Contributions are licensed under the Apache License, Version 2.0, as the
  license's section 5 describes.

## Users of this crate

`meridian-routing` (the route compiler) and `meridian-tiles` (the vector
compiler) build on this crate. A change that alters what they read from a PBF
changes their output, so check both before releasing it. To build either
against a local checkout of this repository, add an override to its
`.cargo/config.toml` (untracked):

```toml
[patch."https://github.com/Limina-Agency/osm-pbf-ingest"]
osm-pbf-ingest = { path = "../osm-pbf-ingest" }
```

Once this crate is on crates.io and they depend on it from there, the section
is `[patch.crates-io]` instead.

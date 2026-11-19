# Changelog

## 0.3.0 (2026-11-19)

### Packed node store

On a file sorted by ID, pass 1 now builds `NodeLocationStore::Packed`: the
node locations in ID order, in blocks of up to 128 nodes packed with a frame
of reference, about 6 bytes per node instead of 16. Each decode worker packs
its block's nodes and the collector appends them in file order, so there is
no spill, sort or rewrite. On the Małopolskie extract (24.3 million nodes)
the store takes 146 MiB instead of 370 MiB, pass 1 takes 0.6 s instead of
0.9 s on four threads, and a lookup about 80 ns instead of 140 ns.

- Breaking: `NodeLocationStore` has a third variant, `Packed(PackedNodeStore)`.
  `NodeStoreStrategy` still picks where the store lives: `Ram`, or `Disk`, a
  mapped file `temp_dir/node_store/nodes.packed` that the store deletes when
  dropped.
- A file whose node IDs do not increase from block to block falls back to
  the sharded `Ram` or `Disk` store, as before; so does a store reopened with
  `NodeLocationStore::open_disk`. `persist_to` writes the same shard files for
  every variant.
- `NodeLocationStore::cursor` returns a `NodeCursor`, which answers like
  `get` and skips the block search while IDs stay in one block.
- `NodeLocationStore::may_touch(node_ids, areas)` says whether nodes may lie
  in any of the `DecimicroBox`es, from the packed blocks' boxes, without
  looking the nodes up (always true for the sharded stores).
- `node_store::decode_decimicro` converts 1e-7 degree units as
  `decode_lat_lng` does.

### Reading one area

- `PrepareOptions::keep_nodes_within(boxes)` keeps only the tagged nodes
  inside the boxes, so a caller reading one area can skip the node blobs in
  pass 2.
- `Pass2Options::keep_ways_where(filter)` takes a `WayFilter`, a closure that
  sees each way's ID, node IDs and borrowed tags (`WayRef`) on the decode
  workers. With `may_touch` it drops the ways outside an area before they get
  owned tags.
- Breaking: `PrepareOptions` and `Pass2Options` are no longer `Copy`, since
  they can hold the boxes and the filter; clone them instead.

## 0.2.0 (2026-11-17)

### No more `osmpbfreader` at runtime

`osmpbfreader` and `protobuf` are now dev-dependencies only (the tests still
check the decoder against `osmpbfreader`). That drops the unmaintained
`smartstring` and, with it, `futures` 0.1, `futures-cpupool`, `par-map`,
`pub-iterator-type`, `flat_map` and `self_cell` from the dependency tree.

- Breaking: `Tags` is the crate's own type (`NodeTags` stays as another name
  for it). Keys and values are `compact_str` strings, so ones up to 24 bytes
  are stored inline as before; the pairs are still sorted by key, and a key
  the file repeats resolves as before. `get` returns `Option<&str>`, so drop
  a `.map(|value| value.as_str())` after it; `iter` yields `(&str, &str)`.
  `insert` takes `&str` and returns nothing.
- Breaking: `blob_index::read_blob_at` returns `blob_index::Blob`, generated
  by buffa from `proto/fileformat.proto`, and its payload is the
  `blob_index::BlobData` enum. A buffa release that changes generated
  messages is now a breaking change for this crate.
- The blob framing and file header are decoded with buffa, and pass 2 walks
  the file's blobs with the crate's own reader. Unlike `protobuf`, buffa does
  not reject a `BlobHeader` or `HeaderBBox` with a `required` field missing.

## 0.1.0 (2026-11-17)

First release on crates.io, as `osm-pbf-ingest`. Until now the crate was
`meridian-osm-ingest`, used from Git: depend on `osm-pbf-ingest` and import
`osm_pbf_ingest` instead.

### Progress share and cancel error

Ported from `meridian-routing`, where it landed after the split (the route
compiler's progress bar and cancel button need it):

- `SharedIngestProgress::fraction`: the share of the pass's blobs handled,
  0 to 1, for pass 1 and for pass 2 with `Pass2Options::skip_nodes` (the
  passes that plan their read set from the blob index); `None` for passes
  that stream the whole file. Breaking for code that builds the struct.
- `INGEST_CANCELLED`, the error every pass returns when `should_cancel`
  stopped it.
- `IngestBuildProfile::worker_threads` is public: the worker count a pass
  uses when the options set none.
- Pass 1 checks for a cancel once more before sorting the node store.

The routing copy's `prepare_ingest_index_keeping_nodes_with_threads` and
`run_pass2_ways_and_relations_with_threads` are not ported: they took owned
`Tags` filters, and `PrepareOptions` / `Pass2Options` cover them.

### Code layout

`lib.rs` is now the crate docs and re-exports. Pass 1 and pass 2 live in
their own modules, and the reader, decode-worker and in-order collector
threads both passes ran as separate copies are one `pipeline` module with its
own tests. Output is unchanged (checked byte-for-byte on five extracts).

- Breaking: removed `run_shared_ingest` and `SharedIngestSink`, the
  three-pass ingest that decoded every object through `osmpbfreader`. Nothing
  called them; use pass 1 and pass 2. `IngestStage::Pass1Relations`,
  `Pass2Ways` and `Pass3Nodes` are no longer reported, and stay only so
  serialized metrics that name them still deserialize.
- A `Pass2Sink` error now stops the reader and workers and waits for them
  before pass 2 returns, as a decode error or a cancel already did.
- When a worker fails to decode a block, pass 1 and pass 2 report that error
  after handing on every block before it, instead of the first error to
  arrive.
- A panic while decoding a block is now that block's error. Before, the
  pass kept decoding the rest of the file, holding every later block in
  memory, and only reported the panic at the end.

### buffa block decoding

Pass 1, pass 2, blob classification and `PbfObjectReader` decode
`PrimitiveBlock`s with buffa views over `proto/osmformat.proto`, a copy of
the OSM schema trimmed to the fields this crate reads. Object metadata and
changesets are skipped unparsed, string tables and packed arrays are read in
place, and each worker reuses one inflate buffer. Output is byte-identical
(pass 1 with and without a node filter, pass 2 full and ways-only, checked
on five real extracts). On 95–194 MiB extracts:

- pass 1: 1.17–1.31x faster;
- pass 2, every object: 1.13–1.22x faster;
- pass 2, ways and relations with a way filter: 1.43–1.83x faster.

A string-table index past the end of the table is now an error instead of a
panic in a worker thread. The generated code is checked in under
`src/proto/`; regenerate it with `scripts/gen-proto.sh`. The crate depends
on buffa 0.9.2 from crates.io. The speedups above were measured with buffa's
proposed `PackedView` (anthropics/buffa#815 and #816), which reads the packed
fields of nodes, ways and relations without a `Vec` per field; on 0.9.2,
which decodes them into `Vec`s, pass 2 over every object is 15–20% slower
than that.

### Borrowed tag filters

- New `TagsRef`, an object's tags borrowed from its block, and `TagLookup`,
  implemented by `TagsRef` and `Tags`, for helpers shared by filters and sinks.
- `PrepareOptions::keep_nodes` now takes `fn(&TagsRef) -> bool`, so pass 1
  builds owned tags only for the nodes it keeps.
- New `Pass2Options` and `run_pass2_with_options`: worker count, `skip_nodes`
  and a `keep_ways` filter over `TagsRef`. `run_pass2` and
  `run_pass2_with_threads` are shorthands for it.
- Deprecated `prepare_ingest_index_keeping_nodes` and
  `run_pass2_ways_and_relations`, whose filters take owned `Tags`. They still
  work, and keep exactly what the new filters keep.

### Pass 1 speedups

`prepare_ingest_index*` produces the same output as before (checked
byte-for-byte on five real extracts), about 10% faster on 95–194 MiB
regional extracts with the default disk node store:

- Decode workers insert node locations into the store themselves. Before, the
  single in-order collector thread did it, and that was the slowest part of
  pass 1.
- Dense nodes are decoded straight from the packed arrays. Pass 1 no longer
  builds an `osmpbfreader::Node` for every node, or allocates tag strings
  unless a `keep_node` filter needs them.

Desktop builds still keep the node store in RAM only for inputs up to
32 MiB. Callers with memory to spare can pick the RAM store for larger
inputs with `PrepareOptions::node_store`, which skips the disk store's
spill and rewrite at about twice the PBF size in RAM.

### API

- New `PrepareOptions` and `prepare_ingest_index_with_options`: worker count,
  node-store strategy and `keep_node` filter in any combination. The existing
  `prepare_ingest_index*` functions are shorthands for it.
- New `node_store::encode_decimicro` and `NodeWorkerChunk::push_decimicro`.
- New metric `PreparedIngestMetrics::node_insert_ms`, summed across workers.
  `collector_node_push_ms` is now always 0.
- Breaking: `NodeLocationStoreBuilder` is now an opaque struct instead of an
  enum, and `push_chunk` takes `&self` so threads can share the builder.

Moved out of the `meridian-routing` workspace into its own repository, with
its history. The code is unchanged from `meridian-routing` at `c4b88ea`.

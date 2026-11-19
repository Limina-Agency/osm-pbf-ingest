//! OSM PBF ingest for map and routing compilers.
//!
//! An ingest is two passes over the file:
//!
//! 1. **Pass 1** ([`prepare_ingest_index_with_options`]) builds the
//!    node-location store and the relation indexes, and can keep the tagged
//!    nodes a filter accepts. It returns a [`PreparedIngestIndex`].
//! 2. **Pass 2** ([`run_pass2_with_options`]) hands nodes, ways and relations
//!    to a [`Pass2Sink`] in file order, one block's batch at a time. Options
//!    skip the node blobs and filter ways.
//!
//! Both passes decode blocks on a pool of worker threads. Filters such as
//! [`PrepareOptions::keep_nodes`] and [`Pass2Options::keep_ways`] read
//! borrowed [`TagsRef`]s, so rejected objects never get owned tags.
//!
//! ```no_run
//! use osm_pbf_ingest::{
//!     IngestBuildProfile, NodeBatch, Pass2Options, Pass2Sink, PrepareOptions, RelationBatch,
//!     WayBatch, prepare_ingest_index_with_options, run_pass2_with_options,
//! };
//! use std::path::Path;
//!
//! #[derive(Default)]
//! struct Roads {
//!     ways: usize,
//! }
//!
//! impl Pass2Sink for Roads {
//!     fn on_relation_batch(&mut self, _batch: RelationBatch) -> Result<(), String> {
//!         Ok(())
//!     }
//!     fn on_way_batch(&mut self, batch: WayBatch) -> Result<(), String> {
//!         self.ways += batch.ways.len();
//!         Ok(())
//!     }
//!     fn on_node_batch(&mut self, _batch: NodeBatch) -> Result<(), String> {
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), String> {
//! let pbf = Path::new("monaco.osm.pbf");
//! let profile = IngestBuildProfile::DesktopFull;
//! let index = prepare_ingest_index_with_options(
//!     pbf,
//!     profile,
//!     Path::new("/tmp/ingest"),
//!     PrepareOptions::new().keep_nodes(|tags| tags.contains_key("barrier")),
//!     |_progress| {},
//!     || false, // return true to cancel
//! )?;
//! let mut roads = Roads::default();
//! run_pass2_with_options(
//!     pbf,
//!     profile,
//!     Pass2Options::new()
//!         .skip_nodes()
//!         .keep_ways(|tags| tags.contains_key("highway")),
//!     &mut roads,
//!     |_progress| {},
//!     || false,
//! )?;
//! println!("{} barriers, {} roads", index.tagged_nodes.len(), roads.ways);
//! # Ok(())
//! # }
//! ```
//!
//! # Decoding
//!
//! Blocks are decoded with buffa views over a trimmed copy of the OSM schema
//! (`proto/`). The blob framing uses the same generated code, and
//! [`blob_index::read_blob_at`] returns its [`blob_index::Blob`], so a buffa
//! release that changes generated messages is a breaking change for this
//! crate.
//!
//! # Where things live
//!
//! | Module | What it holds |
//! |---|---|
//! | `pass1`, `pass2` | the two passes: options, entry points, per-block decoding |
//! | `pipeline` | the reader, decode-worker and in-order collector threads both passes run |
//! | `osm_block` | decoding a `PrimitiveBlock` and walking its nodes, ways and relations |
//! | `model`, `progress`, `profile`, `tags` | the types the passes hand out and take |
//! | [`blob_index`] | the file's blob framing, and which blobs hold which object kinds |
//! | [`node_store`] | the node-location stores pass 1 builds: packed (`packed_node_store`) for files sorted by ID, sharded otherwise |
//! | [`object_lookup`] | finding one object by ID in a sorted file |

mod binary_layout;
pub mod blob_index;
mod model;
pub mod node_store;
pub mod object_lookup;
mod osm_block;
pub mod packed_ids;
mod packed_node_store;
pub mod packed_tags;
mod pass1;
mod pass2;
mod pipeline;
mod profile;
mod progress;
pub mod relation_index;
mod tags;
#[cfg(test)]
mod test_pbf;

pub use model::{
    NodeBatch, NormalizedNode, NormalizedRelation, NormalizedRelationMember, NormalizedWay,
    RelationBatch, RelationMemberType, WayBatch,
};
#[allow(deprecated)]
pub use pass1::prepare_ingest_index_keeping_nodes;
pub use pass1::{
    PrepareOptions, PreparedIngestIndex, PreparedIngestMetrics, prepare_ingest_index,
    prepare_ingest_index_with_options, prepare_ingest_index_with_threads,
};
#[allow(deprecated)]
pub use pass2::run_pass2_ways_and_relations;
pub use pass2::{
    Pass2Options, Pass2Sink, WayFilter, WayRef, run_pass2, run_pass2_with_options,
    run_pass2_with_threads,
};
pub use profile::IngestBuildProfile;
pub use progress::{
    INGEST_CANCELLED, IngestStage, IngestStageMetric, SharedIngestProgress, SharedIngestSummary,
};
pub use tags::{TagLookup, Tags, TagsIter, TagsRef};

/// The name [`Tags`] had while it was `osmpbfreader`'s type.
pub use tags::Tags as NodeTags;

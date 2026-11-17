//! Blob-level index over a PBF file.
//!
//! A PBF is a sequence of length-prefixed frames:
//!
//! ```text
//! u32 big-endian   length of the following BlobHeader
//! BlobHeader       { type: "OSMHeader" | "OSMData", datasize }
//! Blob             datasize bytes, raw or zlib-compressed
//! ```
//!
//! Scanning that framing requires reading only the two headers per frame and
//! seeking past the payload — no decompression, so a full scan costs a few
//! thousand small reads even on a region-sized extract. The scan yields the
//! byte offset of every data blob, which lets later passes seek directly to the
//! blobs they care about instead of streaming the whole file again.
//!
//! It also recovers the `OSMHeader` blob. `osmpbfreader`'s blob iterator drops
//! that frame on the floor (it returns `Ok(None)` for any non-`OSMData` type),
//! so the file's declared features — most importantly `Sort.Type_then_ID` — are
//! unreachable through the API the rest of this crate uses. Several planned
//! optimisations are only sound on a type-then-id-sorted file, and guessing is
//! not an option, so we parse the header ourselves.

use crate::osm_block::Block;
use osmpbfreader::fileformat::{Blob, BlobHeader};
use osmpbfreader::osmformat::HeaderBlock;
use protobuf::Message;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use tracing::warn;

/// Upper bound on a `BlobHeader`, matching the limit `osmpbfreader` enforces.
/// The spec says a header is at most 64 KiB; anything larger means we have lost
/// framing sync and are reading arbitrary bytes as a length.
const MAX_BLOB_HEADER_BYTES: u32 = 64 * 1024;

/// Upper bound on a blob payload. The spec caps uncompressed blobs at 32 MiB;
/// we allow generous headroom and still refuse absurd values, because this
/// number is used to seek and a corrupt one would send us past EOF.
const MAX_BLOB_PAYLOAD_BYTES: i32 = 64 * 1024 * 1024;

const BLOB_TYPE_HEADER: &str = "OSMHeader";
const BLOB_TYPE_DATA: &str = "OSMData";

/// Required-feature strings this compiler knows how to honour. A file that
/// requires anything else cannot be read correctly, and saying so up front beats
/// producing quietly wrong output.
const SUPPORTED_REQUIRED_FEATURES: [&str; 2] = ["OsmSchema-V0.6", "DenseNodes"];

const FEATURE_SORT_TYPE_THEN_ID: &str = "Sort.Type_then_ID";
const FEATURE_LOCATIONS_ON_WAYS: &str = "LocationsOnWays";

#[derive(Debug)]
pub enum PbfScanError {
    Io {
        path: String,
        source: io::Error,
    },
    /// The file ended mid-frame. Carries the offset of the frame that was cut
    /// short so a partial download can be reported against a byte position.
    Truncated {
        offset: u64,
    },
    /// A `BlobHeader` length that cannot be right — we are no longer framed.
    BlobHeaderTooLarge {
        offset: u64,
        len: u32,
    },
    /// A blob payload length that cannot be right, or is negative.
    BlobPayloadOutOfRange {
        offset: u64,
        datasize: i32,
    },
    MalformedBlobHeader {
        offset: u64,
        detail: String,
    },
    MalformedHeaderBlock {
        detail: String,
    },
    MalformedPrimitiveBlock {
        offset: u64,
        detail: String,
    },
    /// No `OSMHeader` frame anywhere in the file.
    MissingHeaderBlob,
    /// The blob uses a compression we cannot decode (lzma, bzip2).
    UnsupportedCompression {
        offset: u64,
    },
}

impl fmt::Display for PbfScanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(formatter, "Failed to read {}: {}", path, source),
            Self::Truncated { offset } => write!(
                formatter,
                "PBF ends mid-frame at byte {}; the file is truncated or still downloading",
                offset
            ),
            Self::BlobHeaderTooLarge { offset, len } => write!(
                formatter,
                "PBF blob header at byte {} declares {} bytes (max {}); the file is corrupt",
                offset, len, MAX_BLOB_HEADER_BYTES
            ),
            Self::BlobPayloadOutOfRange { offset, datasize } => write!(
                formatter,
                "PBF blob at byte {} declares a payload of {} bytes; the file is corrupt",
                offset, datasize
            ),
            Self::MalformedBlobHeader { offset, detail } => write!(
                formatter,
                "Failed to decode PBF blob header at byte {}: {}",
                offset, detail
            ),
            Self::MalformedHeaderBlock { detail } => {
                write!(
                    formatter,
                    "Failed to decode PBF OSMHeader block: {}",
                    detail
                )
            }
            Self::MalformedPrimitiveBlock { offset, detail } => write!(
                formatter,
                "Failed to decode PBF data block at byte {}: {}",
                offset, detail
            ),
            Self::MissingHeaderBlob => {
                write!(formatter, "PBF contains no OSMHeader block")
            }
            Self::UnsupportedCompression { offset } => write!(
                formatter,
                "PBF blob at byte {} uses an unsupported compression (only raw and zlib are supported)",
                offset
            ),
        }
    }
}

impl std::error::Error for PbfScanError {}

/// Bounding box declared in the file header, in degrees.
///
/// Deliberately a plain struct rather than a geo crate's bounding box type:
/// this crate has no geo dependency and a header bbox is advisory metadata,
/// not geometry we compute with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PbfHeaderBounds {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PbfFileHeader {
    pub bounds: Option<PbfHeaderBounds>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub writing_program: Option<String>,
    pub source: Option<String>,
    pub replication_timestamp: Option<i64>,
    pub replication_sequence_number: Option<i64>,
}

impl PbfFileHeader {
    /// True when objects are ordered nodes, then ways, then relations, each
    /// ascending by id.
    ///
    /// This is the precondition for positional node addressing and for locating
    /// object-type boundaries by binary search over blobs. Geofabrik extracts
    /// declare it; files rewritten by some tools do not.
    pub fn sorted_type_then_id(&self) -> bool {
        self.has_optional_feature(FEATURE_SORT_TYPE_THEN_ID)
    }

    /// True when ways carry their nodes' coordinates inline, which makes the
    /// node-location pass unnecessary.
    pub fn locations_on_ways(&self) -> bool {
        self.has_optional_feature(FEATURE_LOCATIONS_ON_WAYS)
    }

    pub fn has_optional_feature(&self, feature: &str) -> bool {
        self.optional_features
            .iter()
            .any(|declared| declared == feature)
    }

    /// Required features we do not implement. Non-empty means the file cannot
    /// be read correctly — most commonly `HistoricalInformation`, which marks a
    /// full-history file whose objects carry multiple versions.
    pub fn unsupported_required_features(&self) -> Vec<&str> {
        self.required_features
            .iter()
            .map(String::as_str)
            .filter(|feature| !SUPPORTED_REQUIRED_FEATURES.contains(feature))
            .collect()
    }
}

/// Where one data blob lives in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLocation {
    /// Position among data blobs, zero-based. Header blobs are not counted.
    pub index: usize,
    /// Byte offset of the blob payload — that is, past the length prefix and
    /// past the `BlobHeader`. Seek here and read `payload_len` bytes.
    pub payload_offset: u64,
    pub payload_len: u32,
}

/// Which object types a blob may contain.
///
/// Defaults to "anything", because an unclassified index must never cause a
/// pass to skip a blob that turns out to hold objects it needed. Classification
/// can only ever narrow this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockContents {
    pub has_nodes: bool,
    pub has_ways: bool,
    pub has_relations: bool,
}

impl Default for BlockContents {
    fn default() -> Self {
        Self {
            has_nodes: true,
            has_ways: true,
            has_relations: true,
        }
    }
}

impl BlockContents {
    pub fn contains(self, kind: ObjectKind) -> bool {
        match kind {
            ObjectKind::Node => self.has_nodes,
            ObjectKind::Way => self.has_ways,
            ObjectKind::Relation => self.has_relations,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Node,
    Way,
    Relation,
}

/// How a blob index came by its content flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifyMethod {
    /// The file declares `Sort.Type_then_ID`, so the type boundaries were found
    /// by binary search — a logarithmic number of blob decompressions.
    BinarySearch,
    /// Every blob was decompressed and inspected.
    Exhaustive,
    /// The file is not sorted by type, so no narrowing was attempted and every
    /// blob is still assumed to hold every type.
    Assumed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassifySummary {
    pub method: ClassifyMethod,
    /// Blobs actually decompressed to reach this classification.
    pub blobs_decompressed: usize,
    pub blobs_with_nodes: usize,
    pub blobs_with_ways: usize,
    pub blobs_with_relations: usize,
}

#[derive(Debug, Clone)]
pub struct PbfBlobIndex {
    header: PbfFileHeader,
    data_blobs: Vec<BlobLocation>,
    contents: Vec<BlockContents>,
    file_len: u64,
}

impl PbfBlobIndex {
    /// Walk the whole file's framing without decompressing any payload.
    pub fn scan(path: &Path) -> Result<Self, PbfScanError> {
        let file = File::open(path).map_err(|source| PbfScanError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let file_len = file
            .metadata()
            .map_err(|source| PbfScanError::Io {
                path: path.display().to_string(),
                source,
            })?
            .len();
        let mut reader = BufReader::new(file);
        let mut index = Self::scan_reader(&mut reader)?;
        // `scan_reader` derives this from the stream; for a real file prefer the
        // authoritative metadata length.
        index.file_len = file_len;
        Ok(index)
    }

    /// Scan an already-open stream. Split out from [`Self::scan`] so tests can
    /// drive it with an in-memory file.
    pub fn scan_reader<R: Read + Seek>(reader: &mut R) -> Result<Self, PbfScanError> {
        let mut header = None;
        let mut data_blobs = Vec::new();
        let start = reader
            .stream_position()
            .map_err(|source| PbfScanError::Io {
                path: "<stream>".to_string(),
                source,
            })?;
        // Seeking past the end of a stream succeeds — `Cursor` and `File` both
        // allow it — so a skip alone can never prove a payload was really
        // present. Measure the stream once and bounds-check every frame against
        // it instead.
        let stream_len = reader
            .seek(SeekFrom::End(0))
            .map_err(|source| PbfScanError::Io {
                path: "<stream>".to_string(),
                source,
            })?;
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|source| PbfScanError::Io {
                path: "<stream>".to_string(),
                source,
            })?;
        let mut offset = start;

        loop {
            let frame_start = offset;
            let Some(blob_header) = read_blob_header(reader, &mut offset)? else {
                break;
            };
            let datasize = blob_header.datasize();
            if !(0..=MAX_BLOB_PAYLOAD_BYTES).contains(&datasize) {
                return Err(PbfScanError::BlobPayloadOutOfRange {
                    offset: frame_start,
                    datasize,
                });
            }
            let payload_len = datasize as u32;
            let payload_offset = offset;
            ensure_available(payload_offset, payload_len, stream_len)?;

            match blob_header.type_() {
                BLOB_TYPE_DATA => {
                    data_blobs.push(BlobLocation {
                        index: data_blobs.len(),
                        payload_offset,
                        payload_len,
                    });
                    skip(reader, payload_len, &mut offset, payload_offset)?;
                }
                BLOB_TYPE_HEADER => {
                    // The one frame we do decompress. It is a few hundred bytes
                    // and it decides whether the fast paths are legal.
                    let blob = read_blob_payload(reader, payload_len, &mut offset, payload_offset)?;
                    let bytes = decode_blob_bytes(&blob, payload_offset)?;
                    let parsed = HeaderBlock::parse_from_bytes(&bytes).map_err(|error| {
                        PbfScanError::MalformedHeaderBlock {
                            detail: error.to_string(),
                        }
                    })?;
                    if header.is_none() {
                        header = Some(header_from_block(parsed));
                    } else {
                        warn!(
                            offset = payload_offset,
                            "PBF contains more than one OSMHeader block; ignoring the later one"
                        );
                    }
                }
                other => {
                    // The spec reserves unknown types for future use and tells
                    // readers to skip them.
                    warn!(
                        blob_type = other,
                        offset = payload_offset,
                        "Skipping unknown PBF blob type"
                    );
                    skip(reader, payload_len, &mut offset, payload_offset)?;
                }
            }
        }

        let header = header.ok_or(PbfScanError::MissingHeaderBlob)?;
        let contents = vec![BlockContents::default(); data_blobs.len()];
        Ok(Self {
            header,
            data_blobs,
            contents,
            file_len: stream_len,
        })
    }

    /// Narrow each blob's content flags so later passes can skip blobs that
    /// cannot hold the object type they want.
    ///
    /// On a `Sort.Type_then_ID` file the blobs are laid out nodes, then ways,
    /// then relations, so the two transition points can be found by binary
    /// search and only `O(log n)` blobs need decompressing. Boundary blobs get
    /// both neighbouring types, since a single blob may straddle a transition.
    ///
    /// On an unsorted file no narrowing is sound, so this leaves every blob
    /// marked as holding everything and reports [`ClassifyMethod::Assumed`].
    /// Callers that want the narrowing anyway can pay for
    /// [`Self::classify_exhaustive`].
    pub fn classify(&mut self, path: &Path) -> Result<ClassifySummary, PbfScanError> {
        let mut reader = open_for_read(path)?;
        self.classify_reader(&mut reader)
    }

    pub fn classify_reader<R: Read + Seek>(
        &mut self,
        reader: &mut R,
    ) -> Result<ClassifySummary, PbfScanError> {
        if !self.header.sorted_type_then_id() {
            return Ok(self.summarize(ClassifyMethod::Assumed, 0));
        }
        if self.data_blobs.is_empty() {
            return Ok(self.summarize(ClassifyMethod::BinarySearch, 0));
        }

        let mut decompressed = 0usize;

        // First blob holding a way or a relation: everything before it is nodes.
        let ways_start =
            first_matching_blob(&self.data_blobs, reader, &mut decompressed, |contents| {
                contents.has_ways || contents.has_relations
            })?;
        // First blob holding a relation.
        let relations_start =
            first_matching_blob(&self.data_blobs, reader, &mut decompressed, |contents| {
                contents.has_relations
            })?;

        for (index, contents) in self.contents.iter_mut().enumerate() {
            // Inclusive on both sides on purpose: the blob at a transition may
            // hold objects of both the type ending and the type beginning, and
            // over-reading two blobs is free next to missing an object.
            *contents = BlockContents {
                has_nodes: index <= ways_start,
                has_ways: index >= ways_start && index <= relations_start,
                has_relations: index >= relations_start,
            };
        }

        Ok(self.summarize(ClassifyMethod::BinarySearch, decompressed))
    }

    /// Decompress and inspect every blob. Correct for any file regardless of
    /// sort order, and far more expensive — mainly useful for verifying the
    /// binary search and for unsorted inputs where the narrowing is worth
    /// paying for.
    pub fn classify_exhaustive<R: Read + Seek>(
        &mut self,
        reader: &mut R,
    ) -> Result<ClassifySummary, PbfScanError> {
        for (index, location) in self.data_blobs.iter().enumerate() {
            self.contents[index] = blob_contents(reader, location)?;
        }
        let decompressed = self.data_blobs.len();
        Ok(self.summarize(ClassifyMethod::Exhaustive, decompressed))
    }

    fn summarize(&self, method: ClassifyMethod, blobs_decompressed: usize) -> ClassifySummary {
        ClassifySummary {
            method,
            blobs_decompressed,
            blobs_with_nodes: self.count_containing(ObjectKind::Node),
            blobs_with_ways: self.count_containing(ObjectKind::Way),
            blobs_with_relations: self.count_containing(ObjectKind::Relation),
        }
    }

    fn count_containing(&self, kind: ObjectKind) -> usize {
        self.contents
            .iter()
            .filter(|contents| contents.contains(kind))
            .count()
    }

    /// Content flags for one data blob, by its index.
    pub fn contents(&self, index: usize) -> BlockContents {
        self.contents
            .get(index)
            .copied()
            .unwrap_or_else(BlockContents::default)
    }

    /// The blobs a pass over `kind` needs to read, in file order.
    pub fn blobs_containing(&self, kind: ObjectKind) -> impl Iterator<Item = &BlobLocation> + '_ {
        self.data_blobs
            .iter()
            .filter(move |location| self.contents(location.index).contains(kind))
    }

    /// The blobs a pass needs when it consumes several object types, in file
    /// order and without duplicates. A pass that reads two types wants the
    /// union, not two separate traversals.
    pub fn blobs_containing_any<'a>(
        &'a self,
        kinds: &'a [ObjectKind],
    ) -> impl Iterator<Item = &'a BlobLocation> + 'a {
        self.data_blobs.iter().filter(move |location| {
            let contents = self.contents(location.index);
            kinds.iter().any(|kind| contents.contains(*kind))
        })
    }

    pub fn header(&self) -> &PbfFileHeader {
        &self.header
    }

    pub fn data_blobs(&self) -> &[BlobLocation] {
        &self.data_blobs
    }

    pub fn len(&self) -> usize {
        self.data_blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data_blobs.is_empty()
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Mean compressed payload bytes per data blob.
    ///
    /// Blocks much larger than ~1 MiB come from writers that pack many more
    /// than the conventional 8000 objects per block, which costs parallelism
    /// because a block is the unit of work. Worth surfacing as advice rather
    /// than silently running slower.
    pub fn mean_blob_payload_bytes(&self) -> u64 {
        if self.data_blobs.is_empty() {
            return 0;
        }
        let total: u64 = self
            .data_blobs
            .iter()
            .map(|blob| u64::from(blob.payload_len))
            .sum();
        total / self.data_blobs.len() as u64
    }
}

fn open_for_read(path: &Path) -> Result<BufReader<File>, PbfScanError> {
    let file = File::open(path).map_err(|source| PbfScanError::Io {
        path: path.display().to_string(),
        source,
    })?;
    Ok(BufReader::new(file))
}

/// Index of the first blob whose contents satisfy `predicate`, or `len` if none
/// do.
///
/// This is a binary search, so it is only correct when the predicate is monotone
/// over the blobs — false for a prefix, then true for the rest. That holds
/// exactly because the file declared `Sort.Type_then_ID`; callers must not use
/// it otherwise.
fn first_matching_blob<R: Read + Seek>(
    blobs: &[BlobLocation],
    reader: &mut R,
    decompressed: &mut usize,
    predicate: impl Fn(BlockContents) -> bool,
) -> Result<usize, PbfScanError> {
    let mut low = 0usize;
    let mut high = blobs.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let contents = blob_contents(reader, &blobs[mid])?;
        *decompressed += 1;
        if predicate(contents) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(low)
}

/// Decompress one blob and report which object types its primitive groups hold.
fn blob_contents<R: Read + Seek>(
    reader: &mut R,
    location: &BlobLocation,
) -> Result<BlockContents, PbfScanError> {
    let blob = read_blob_at(reader, location)?;
    let mut buffer = Vec::new();
    let bytes = blob_bytes(&blob, location.payload_offset, &mut buffer)?;
    let block = Block::decode(bytes).map_err(|detail| PbfScanError::MalformedPrimitiveBlock {
        offset: location.payload_offset,
        detail,
    })?;

    let mut contents = BlockContents {
        has_nodes: false,
        has_ways: false,
        has_relations: false,
    };
    for group in block.groups() {
        // Nodes arrive either as a repeated Node field or packed into a single
        // DenseNodes message; a block that uses dense encoding has an empty
        // `nodes` vector, so checking only that would miss almost every real
        // node block.
        if !group.nodes.is_empty() || !group.dense.id.is_empty() {
            contents.has_nodes = true;
        }
        if !group.ways.is_empty() {
            contents.has_ways = true;
        }
        if !group.relations.is_empty() {
            contents.has_relations = true;
        }
    }
    Ok(contents)
}

/// Read one blob payload by offset. Used by passes that seek to a subset of
/// blobs instead of streaming the file.
pub fn read_blob_at<R: Read + Seek>(
    reader: &mut R,
    location: &BlobLocation,
) -> Result<Blob, PbfScanError> {
    reader
        .seek(SeekFrom::Start(location.payload_offset))
        .map_err(|source| PbfScanError::Io {
            path: "<stream>".to_string(),
            source,
        })?;
    let mut offset = location.payload_offset;
    read_blob_payload(
        reader,
        location.payload_len,
        &mut offset,
        location.payload_offset,
    )
}

/// Read the next `BlobHeader`, or `None` at a clean end of file.
fn read_blob_header<R: Read + Seek>(
    reader: &mut R,
    offset: &mut u64,
) -> Result<Option<BlobHeader>, PbfScanError> {
    let frame_start = *offset;
    let mut length_bytes = [0u8; 4];
    // A clean end of file is *zero* bytes here. One to three bytes means the
    // file was cut mid-frame, which is a different diagnosis for the user — a
    // partial download rather than a complete file — so count what we got
    // instead of letting `read_exact` collapse both into `UnexpectedEof`.
    match read_up_to(reader, &mut length_bytes)? {
        0 => return Ok(None),
        4 => {}
        _ => {
            return Err(PbfScanError::Truncated {
                offset: frame_start,
            });
        }
    }
    *offset += 4;

    let header_len = u32::from_be_bytes(length_bytes);
    if header_len == 0 || header_len > MAX_BLOB_HEADER_BYTES {
        return Err(PbfScanError::BlobHeaderTooLarge {
            offset: frame_start,
            len: header_len,
        });
    }

    let mut header_bytes = vec![0u8; header_len as usize];
    read_exact_or_truncated(reader, &mut header_bytes, *offset)?;
    *offset += u64::from(header_len);

    let header = BlobHeader::parse_from_bytes(&header_bytes).map_err(|error| {
        PbfScanError::MalformedBlobHeader {
            offset: frame_start,
            detail: error.to_string(),
        }
    })?;
    Ok(Some(header))
}

fn read_blob_payload<R: Read + Seek>(
    reader: &mut R,
    payload_len: u32,
    offset: &mut u64,
    payload_offset: u64,
) -> Result<Blob, PbfScanError> {
    let mut payload = vec![0u8; payload_len as usize];
    read_exact_or_truncated(reader, &mut payload, payload_offset)?;
    *offset += u64::from(payload_len);
    Blob::parse_from_bytes(&payload).map_err(|error| PbfScanError::MalformedBlobHeader {
        offset: payload_offset,
        detail: error.to_string(),
    })
}

/// Uncompress a blob to its encoded protobuf bytes.
pub(crate) fn decode_blob_bytes(blob: &Blob, offset: u64) -> Result<Vec<u8>, PbfScanError> {
    if blob.has_raw() {
        return Ok(blob.raw().to_vec());
    }
    let mut buffer = Vec::new();
    blob_bytes(blob, offset, &mut buffer)?;
    Ok(buffer)
}

/// A blob's encoded protobuf bytes: the payload itself when stored raw,
/// otherwise inflated into `buffer`, whose allocation is reused from call to
/// call.
pub(crate) fn blob_bytes<'b>(
    blob: &'b Blob,
    offset: u64,
    buffer: &'b mut Vec<u8>,
) -> Result<&'b [u8], PbfScanError> {
    if blob.has_raw() {
        return Ok(blob.raw());
    }
    if blob.has_zlib_data() {
        use flate2::read::ZlibDecoder;
        buffer.clear();
        // `raw_size` comes from the file, so it only sizes the first
        // allocation up to the spec's cap; a larger block still inflates.
        let expected = usize::try_from(blob.raw_size().max(0)).unwrap_or(0);
        buffer.reserve(expected.min(MAX_BLOB_PAYLOAD_BYTES as usize));
        ZlibDecoder::new(blob.zlib_data())
            .read_to_end(buffer)
            .map_err(|source| PbfScanError::Io {
                path: "<blob>".to_string(),
                source,
            })?;
        return Ok(buffer);
    }
    Err(PbfScanError::UnsupportedCompression { offset })
}

/// Reject a frame that claims more bytes than the stream holds, before we try
/// to seek or allocate for it.
fn ensure_available(offset: u64, len: u32, stream_len: u64) -> Result<(), PbfScanError> {
    match offset.checked_add(u64::from(len)) {
        Some(end) if end <= stream_len => Ok(()),
        _ => Err(PbfScanError::Truncated { offset }),
    }
}

fn skip<R: Read + Seek>(
    reader: &mut R,
    payload_len: u32,
    offset: &mut u64,
    payload_offset: u64,
) -> Result<(), PbfScanError> {
    // Seek rather than read: skipping payloads is the whole point of the scan.
    // The span was bounds-checked against the stream length by the caller, so a
    // successful seek here really does land inside the file.
    let position = reader
        .seek(SeekFrom::Start(payload_offset + u64::from(payload_len)))
        .map_err(|source| PbfScanError::Io {
            path: "<stream>".to_string(),
            source,
        })?;
    *offset = position;
    Ok(())
}

/// Read until the buffer is full or the stream ends, returning how many bytes
/// were actually read. `read_exact` cannot express "ended early, and here is
/// how early", which is the distinction between a finished file and a partial
/// one.
fn read_up_to<R: Read>(reader: &mut R, buffer: &mut [u8]) -> Result<usize, PbfScanError> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(source) => {
                return Err(PbfScanError::Io {
                    path: "<stream>".to_string(),
                    source,
                });
            }
        }
    }
    Ok(filled)
}

/// Turns a short read into a truncation error rather than a decode failure, so
/// a partial download is not reported as a corrupt file.
fn read_exact_or_truncated<R: Read>(
    reader: &mut R,
    buffer: &mut [u8],
    offset: u64,
) -> Result<(), PbfScanError> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(PbfScanError::Truncated { offset })
        }
        Err(source) => Err(PbfScanError::Io {
            path: "<stream>".to_string(),
            source,
        }),
    }
}

fn header_from_block(block: HeaderBlock) -> PbfFileHeader {
    // HeaderBBox is in nanodegrees.
    const NANODEGREES: f64 = 1_000_000_000.0;
    let bounds = block.bbox.as_ref().map(|bbox| PbfHeaderBounds {
        west: bbox.left() as f64 / NANODEGREES,
        south: bbox.bottom() as f64 / NANODEGREES,
        east: bbox.right() as f64 / NANODEGREES,
        north: bbox.top() as f64 / NANODEGREES,
    });
    PbfFileHeader {
        bounds,
        required_features: block.required_features.clone(),
        optional_features: block.optional_features.clone(),
        writing_program: block
            .writingprogram
            .as_ref()
            .map(|program| program.to_string()),
        source: block.source.as_ref().map(|source| source.to_string()),
        replication_timestamp: block.osmosis_replication_timestamp,
        replication_sequence_number: block.osmosis_replication_sequence_number,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osmpbfreader::osmformat::{
        DenseNodes, HeaderBBox, Node as OsmNode, PrimitiveBlock, PrimitiveGroup,
        Relation as OsmRelation, StringTable, Way as OsmWay,
    };
    use std::io::Cursor;

    /// Build a PBF frame: length-prefixed BlobHeader, then the blob payload.
    fn frame(blob_type: &str, payload: &[u8]) -> Vec<u8> {
        let mut blob = Blob::new();
        blob.set_raw(payload.to_vec());
        blob.set_raw_size(payload.len() as i32);
        let blob_bytes = blob.write_to_bytes().expect("encode blob");

        let mut blob_header = BlobHeader::new();
        blob_header.set_type(blob_type.to_string());
        blob_header.set_datasize(blob_bytes.len() as i32);
        let header_bytes = blob_header.write_to_bytes().expect("encode blob header");

        let mut out = Vec::new();
        out.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&header_bytes);
        out.extend_from_slice(&blob_bytes);
        out
    }

    fn header_payload(optional_features: &[&str], required_features: &[&str]) -> Vec<u8> {
        let mut block = HeaderBlock::new();
        for feature in optional_features {
            block.optional_features.push((*feature).to_string());
        }
        for feature in required_features {
            block.required_features.push((*feature).to_string());
        }
        let mut bbox = HeaderBBox::new();
        bbox.set_left(18_850_000_000);
        bbox.set_right(21_500_000_000);
        bbox.set_top(50_650_000_000);
        bbox.set_bottom(48_950_000_000);
        block.bbox = Some(bbox).into();
        block.set_writingprogram("osm-pbf-ingest-test".to_string());
        block.write_to_bytes().expect("encode header block")
    }

    fn data_payload(marker: i32) -> Vec<u8> {
        let mut block = PrimitiveBlock::new();
        // `stringtable` is required in the proto2 schema; encoding fails without it.
        block.stringtable = Some(StringTable::new()).into();
        block.set_granularity(marker);
        block.write_to_bytes().expect("encode primitive block")
    }

    /// A data block holding one object of each requested kind. `dense` selects
    /// the packed node encoding real files overwhelmingly use.
    fn typed_payload(kinds: &[ObjectKind], dense: bool) -> Vec<u8> {
        let mut group = PrimitiveGroup::new();
        for kind in kinds {
            match kind {
                ObjectKind::Node if dense => {
                    let mut nodes = DenseNodes::new();
                    nodes.id.push(1);
                    nodes.lat.push(0);
                    nodes.lon.push(0);
                    group.dense = Some(nodes).into();
                }
                ObjectKind::Node => {
                    let mut node = OsmNode::new();
                    node.set_id(1);
                    node.set_lat(0);
                    node.set_lon(0);
                    group.nodes.push(node);
                }
                ObjectKind::Way => {
                    let mut way = OsmWay::new();
                    way.set_id(1);
                    group.ways.push(way);
                }
                ObjectKind::Relation => {
                    let mut relation = OsmRelation::new();
                    relation.set_id(1);
                    group.relations.push(relation);
                }
            }
        }

        let mut block = PrimitiveBlock::new();
        block.stringtable = Some(StringTable::new()).into();
        block.primitivegroup.push(group);
        block.write_to_bytes().expect("encode primitive block")
    }

    /// A file laid out the way a `Sort.Type_then_ID` PBF is: node blocks, then
    /// way blocks, then relation blocks, with straddling blocks at each seam.
    fn sorted_typed_pbf(sorted: bool) -> Vec<u8> {
        let optional: &[&str] = if sorted { &["Sort.Type_then_ID"] } else { &[] };
        let mut bytes = frame(
            BLOB_TYPE_HEADER,
            &header_payload(optional, &["OsmSchema-V0.6", "DenseNodes"]),
        );
        let layout: [&[ObjectKind]; 8] = [
            &[ObjectKind::Node],
            &[ObjectKind::Node],
            &[ObjectKind::Node],
            &[ObjectKind::Node, ObjectKind::Way], // seam
            &[ObjectKind::Way],
            &[ObjectKind::Way, ObjectKind::Relation], // seam
            &[ObjectKind::Relation],
            &[ObjectKind::Relation],
        ];
        for kinds in layout {
            bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, &typed_payload(kinds, true)));
        }
        bytes
    }

    fn test_pbf(data_blocks: usize) -> Vec<u8> {
        let mut bytes = frame(
            BLOB_TYPE_HEADER,
            &header_payload(&["Sort.Type_then_ID"], &["OsmSchema-V0.6", "DenseNodes"]),
        );
        for block in 0..data_blocks {
            bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, &data_payload(block as i32)));
        }
        bytes
    }

    #[test]
    fn scan_records_every_data_blob_and_skips_the_header_blob() {
        let bytes = test_pbf(3);
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert_eq!(index.len(), 3);
        assert_eq!(index.file_len(), bytes.len() as u64);
        for (position, blob) in index.data_blobs().iter().enumerate() {
            assert_eq!(blob.index, position);
        }
    }

    #[test]
    fn recorded_offsets_read_back_the_expected_blob() {
        let bytes = test_pbf(4);
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let mut reader = Cursor::new(&bytes);

        // Out of order, because seeking to an arbitrary blob is the point.
        for position in [3usize, 0, 2, 1] {
            let blob = read_blob_at(&mut reader, &index.data_blobs()[position]).expect("read blob");
            let decoded = decode_blob_bytes(&blob, 0).expect("decode blob");
            let block = PrimitiveBlock::parse_from_bytes(&decoded).expect("parse block");
            assert_eq!(block.granularity(), position as i32);
        }
    }

    #[test]
    fn header_exposes_sort_order_and_bounds() {
        let bytes = test_pbf(1);
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let header = index.header();

        assert!(header.sorted_type_then_id());
        assert!(!header.locations_on_ways());
        assert!(header.unsupported_required_features().is_empty());
        assert_eq!(
            header.writing_program.as_deref(),
            Some("osm-pbf-ingest-test")
        );

        let bounds = header.bounds.expect("bounds");
        assert!((bounds.west - 18.85).abs() < 1e-9);
        assert!((bounds.south - 48.95).abs() < 1e-9);
        assert!((bounds.east - 21.50).abs() < 1e-9);
        assert!((bounds.north - 50.65).abs() < 1e-9);
    }

    #[test]
    fn unsorted_files_are_reported_as_unsorted() {
        let mut bytes = frame(BLOB_TYPE_HEADER, &header_payload(&[], &["OsmSchema-V0.6"]));
        bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, &data_payload(0)));
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert!(!index.header().sorted_type_then_id());
    }

    #[test]
    fn history_files_are_flagged_as_unsupported() {
        let mut bytes = frame(
            BLOB_TYPE_HEADER,
            &header_payload(&[], &["OsmSchema-V0.6", "HistoricalInformation"]),
        );
        bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, &data_payload(0)));
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert_eq!(
            index.header().unsupported_required_features(),
            vec!["HistoricalInformation"]
        );
    }

    #[test]
    fn unknown_blob_types_are_skipped_without_losing_framing() {
        let mut bytes = frame(BLOB_TYPE_HEADER, &header_payload(&[], &["OsmSchema-V0.6"]));
        bytes.extend_from_slice(&frame("OSMSomethingNew", b"opaque"));
        bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, &data_payload(7)));
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert_eq!(index.len(), 1);
        let mut reader = Cursor::new(&bytes);
        let blob = read_blob_at(&mut reader, &index.data_blobs()[0]).expect("read blob");
        let decoded = decode_blob_bytes(&blob, 0).expect("decode blob");
        let block = PrimitiveBlock::parse_from_bytes(&decoded).expect("parse block");
        assert_eq!(block.granularity(), 7);
    }

    #[test]
    fn a_file_cut_mid_payload_reports_truncation_not_a_decode_failure() {
        let bytes = test_pbf(2);
        let cut = bytes.len() - 4;
        let error = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes[..cut]))
            .expect_err("expected truncation");

        assert!(
            matches!(error, PbfScanError::Truncated { .. }),
            "expected Truncated, got {error:?}"
        );
    }

    #[test]
    fn a_file_cut_mid_blob_header_reports_truncation() {
        let bytes = test_pbf(1);
        // Two bytes into the first length prefix.
        let error =
            PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes[..2])).expect_err("expected error");

        assert!(
            matches!(error, PbfScanError::Truncated { .. }),
            "expected Truncated, got {error:?}"
        );
    }

    #[test]
    fn garbage_length_prefixes_are_rejected_before_allocating() {
        let mut bytes = vec![0xff, 0xff, 0xff, 0xff];
        bytes.extend_from_slice(&[0u8; 16]);
        let error =
            PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect_err("expected error");

        assert!(
            matches!(error, PbfScanError::BlobHeaderTooLarge { .. }),
            "expected BlobHeaderTooLarge, got {error:?}"
        );
    }

    #[test]
    fn a_file_without_a_header_blob_is_rejected() {
        let bytes = frame(BLOB_TYPE_DATA, &data_payload(0));
        let error =
            PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect_err("expected error");

        assert!(
            matches!(error, PbfScanError::MissingHeaderBlob),
            "expected MissingHeaderBlob, got {error:?}"
        );
    }

    #[test]
    fn classification_narrows_each_blob_to_the_types_it_can_hold() {
        let bytes = sorted_typed_pbf(true);
        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let summary = index
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        assert_eq!(summary.method, ClassifyMethod::BinarySearch);
        // Node blocks are 0..=3, ways 3..=5, relations 5..=7.
        assert_eq!(summary.blobs_with_nodes, 4);
        assert_eq!(summary.blobs_with_ways, 3);
        assert_eq!(summary.blobs_with_relations, 3);

        assert!(index.contents(0).has_nodes && !index.contents(0).has_ways);
        assert!(index.contents(3).has_nodes && index.contents(3).has_ways);
        assert!(index.contents(5).has_ways && index.contents(5).has_relations);
        assert!(index.contents(7).has_relations && !index.contents(7).has_ways);
    }

    #[test]
    fn binary_search_classification_never_drops_a_blob_the_exhaustive_scan_keeps() {
        let bytes = sorted_typed_pbf(true);

        let mut searched = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        searched
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        let mut exhaustive = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        exhaustive
            .classify_exhaustive(&mut Cursor::new(&bytes))
            .expect("classify exhaustive");

        // The binary search is allowed to be more permissive at the seams, but
        // never less: a blob the ground truth says holds a type must still be
        // read for that type.
        for kind in [ObjectKind::Node, ObjectKind::Way, ObjectKind::Relation] {
            for index in 0..searched.len() {
                if exhaustive.contents(index).contains(kind) {
                    assert!(
                        searched.contents(index).contains(kind),
                        "binary search dropped blob {index} for {kind:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn binary_search_reads_far_fewer_blobs_than_it_classifies() {
        let bytes = sorted_typed_pbf(true);
        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let summary = index
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        assert!(
            summary.blobs_decompressed < index.len(),
            "decompressed {} of {} blobs",
            summary.blobs_decompressed,
            index.len()
        );
    }

    #[test]
    fn dense_node_blocks_are_recognised_as_holding_nodes() {
        // Real files pack nodes into DenseNodes, leaving `nodes` empty; a
        // classifier that only checked `nodes` would mark every node block as
        // empty and the node pass would skip the entire file.
        let mut bytes = frame(
            BLOB_TYPE_HEADER,
            &header_payload(&["Sort.Type_then_ID"], &["OsmSchema-V0.6", "DenseNodes"]),
        );
        bytes.extend_from_slice(&frame(
            BLOB_TYPE_DATA,
            &typed_payload(&[ObjectKind::Node], true),
        ));
        bytes.extend_from_slice(&frame(
            BLOB_TYPE_DATA,
            &typed_payload(&[ObjectKind::Way], false),
        ));

        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        index
            .classify_exhaustive(&mut Cursor::new(&bytes))
            .expect("classify");

        assert!(index.contents(0).has_nodes);
        assert!(!index.contents(0).has_ways);
        assert!(index.contents(1).has_ways);
    }

    #[test]
    fn unsorted_files_keep_every_blob_for_every_pass() {
        let bytes = sorted_typed_pbf(false);
        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let summary = index
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        assert_eq!(summary.method, ClassifyMethod::Assumed);
        assert_eq!(summary.blobs_decompressed, 0);
        assert_eq!(summary.blobs_with_nodes, index.len());
        assert_eq!(summary.blobs_with_ways, index.len());
        assert_eq!(summary.blobs_with_relations, index.len());
    }

    #[test]
    fn an_unclassified_index_assumes_every_blob_holds_everything() {
        let bytes = sorted_typed_pbf(true);
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert_eq!(index.blobs_containing(ObjectKind::Relation).count(), 8);
    }

    #[test]
    fn blobs_containing_yields_the_pass_read_set_in_file_order() {
        let bytes = sorted_typed_pbf(true);
        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        index
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        let relation_blobs: Vec<usize> = index
            .blobs_containing(ObjectKind::Relation)
            .map(|location| location.index)
            .collect();
        assert_eq!(relation_blobs, vec![5, 6, 7]);

        let node_blobs: Vec<usize> = index
            .blobs_containing(ObjectKind::Node)
            .map(|location| location.index)
            .collect();
        assert_eq!(node_blobs, vec![0, 1, 2, 3]);
    }

    #[test]
    fn a_file_with_only_nodes_classifies_without_ways_or_relations() {
        let mut bytes = frame(
            BLOB_TYPE_HEADER,
            &header_payload(&["Sort.Type_then_ID"], &["OsmSchema-V0.6", "DenseNodes"]),
        );
        for _ in 0..3 {
            bytes.extend_from_slice(&frame(
                BLOB_TYPE_DATA,
                &typed_payload(&[ObjectKind::Node], true),
            ));
        }

        let mut index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");
        let summary = index
            .classify_reader(&mut Cursor::new(&bytes))
            .expect("classify");

        assert_eq!(summary.blobs_with_nodes, 3);
        assert_eq!(summary.blobs_with_ways, 0);
        assert_eq!(summary.blobs_with_relations, 0);
    }

    #[test]
    fn mean_blob_payload_bytes_is_zero_for_a_header_only_file() {
        let bytes = frame(BLOB_TYPE_HEADER, &header_payload(&[], &["OsmSchema-V0.6"]));
        let index = PbfBlobIndex::scan_reader(&mut Cursor::new(&bytes)).expect("scan");

        assert!(index.is_empty());
        assert_eq!(index.mean_blob_payload_bytes(), 0);
    }
}

use std::mem::size_of;
use zerocopy::byteorder::little_endian::I64 as LeI64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
pub struct NodeShardRecord {
    node_id: LeI64,
    encoded_lat_lng: LeI64,
}

impl NodeShardRecord {
    pub const SIZE: usize = size_of::<Self>();

    pub fn new(node_id: i64, encoded_lat_lng: i64) -> Self {
        Self {
            node_id: LeI64::new(node_id),
            encoded_lat_lng: LeI64::new(encoded_lat_lng),
        }
    }

    pub fn node_id(self) -> i64 {
        self.node_id.get()
    }

    pub fn encoded_lat_lng(self) -> i64 {
        self.encoded_lat_lng.get()
    }
}

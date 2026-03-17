/// Page size in bytes (4 KB).
pub const PAGE_SIZE: usize = 4096;

/// Invalid page sentinel.
pub const INVALID_PAGE_ID: u32 = u32::MAX;

/// A page identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

/// A record identifier (page + slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RID {
    pub page_id: PageId,
    pub slot_id: u16,
}

/// A table identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

/// A transaction identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnId(pub u64);

/// An index identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexId(pub u32);

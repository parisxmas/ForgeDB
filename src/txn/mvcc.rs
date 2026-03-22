//! Multi-Version Concurrency Control (MVCC) visibility logic.
//!
//! Each tuple version has:
//! - `xmin`: the transaction ID that created it
//! - `xmax`: the transaction ID that deleted/updated it (0 = live)
//!
//! A snapshot captures the state at a point in time and determines which
//! tuple versions are visible to a particular transaction.

use std::collections::HashSet;

use crate::common::TxnId;

/// A consistent snapshot of the transaction state, taken at query start.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The transaction ID of the reader.
    pub txn_id: TxnId,
    /// Set of transaction IDs that were active (uncommitted) when the snapshot was taken.
    pub active_txns: HashSet<TxnId>,
    /// The smallest possible active transaction ID. All txns < this are committed.
    pub xmin: u64,
    /// The next transaction ID that will be assigned. All txns >= this don't exist yet.
    pub xmax: u64,
}

/// MVCC version header prepended to each tuple.
pub const MVCC_HEADER_SIZE: usize = 16; // xmin(8) + xmax(8)

/// Sentinel value meaning "not deleted".
pub const XMAX_NONE: u64 = 0;

/// Serialize an MVCC version header.
pub fn encode_version_header(xmin: u64, xmax: u64) -> [u8; MVCC_HEADER_SIZE] {
    let mut header = [0u8; MVCC_HEADER_SIZE];
    header[0..8].copy_from_slice(&xmin.to_le_bytes());
    header[8..16].copy_from_slice(&xmax.to_le_bytes());
    header
}

/// Deserialize an MVCC version header from a byte slice.
/// Returns (xmin, xmax).
pub fn decode_version_header(data: &[u8]) -> (u64, u64) {
    if data.len() < MVCC_HEADER_SIZE {
        return (0, 0);
    }
    let xmin = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let xmax = u64::from_le_bytes(data[8..16].try_into().unwrap());
    (xmin, xmax)
}

/// Determine if a tuple version is visible to the given snapshot.
///
/// Visibility rules (snapshot isolation):
/// 1. The creating transaction (xmin) must be committed:
///    - xmin < snapshot.xmin (committed before snapshot window), OR
///    - xmin == snapshot.txn_id (created by current transaction), OR
///    - xmin not in active_txns AND xmin < snapshot.xmax (committed during snapshot window)
/// 2. The deleting transaction (xmax), if any, must NOT be committed:
///    - xmax == 0 (not deleted), OR
///    - xmax == snapshot.txn_id (deleted by current txn — but we still see our own deletes as invisible)
///    - xmax is in active_txns (not yet committed), OR
///    - xmax >= snapshot.xmax (started after snapshot)
pub fn is_visible(xmin: u64, xmax: u64, snapshot: &Snapshot) -> bool {
    // Check if xmin is visible (the creating transaction is committed)
    let xmin_visible = if xmin == snapshot.txn_id.0 {
        // Created by our own transaction — visible
        true
    } else if xmin < snapshot.xmin {
        // Created by a transaction that committed before our snapshot window
        true
    } else if xmin >= snapshot.xmax {
        // Created by a transaction that started after our snapshot — invisible
        false
    } else {
        // In the snapshot window: visible only if NOT in active set (i.e., committed)
        !snapshot.active_txns.contains(&TxnId(xmin))
    };

    if !xmin_visible {
        return false;
    }

    // Check if xmax makes it invisible (the row has been deleted/updated)
    if xmax == XMAX_NONE {
        // Not deleted — visible
        return true;
    }

    if xmax == snapshot.txn_id.0 {
        // Deleted by our own transaction — invisible (we see our own deletes)
        return false;
    }

    if xmax >= snapshot.xmax {
        // Deleted by a transaction that started after our snapshot — still visible to us
        return true;
    }

    if snapshot.active_txns.contains(&TxnId(xmax)) {
        // Deleted by an uncommitted transaction — still visible
        return true;
    }

    // Deleted by a committed transaction within our snapshot window — invisible
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(txn_id: u64, active: &[u64], xmin: u64, xmax: u64) -> Snapshot {
        Snapshot {
            txn_id: TxnId(txn_id),
            active_txns: active.iter().map(|&id| TxnId(id)).collect(),
            xmin,
            xmax,
        }
    }

    #[test]
    fn test_visible_committed_before_snapshot() {
        let snap = make_snapshot(10, &[], 1, 11);
        // Tuple created by txn 5 (committed, < xmin), not deleted
        assert!(is_visible(5, XMAX_NONE, &snap));
    }

    #[test]
    fn test_invisible_created_by_active_txn() {
        let snap = make_snapshot(10, &[7], 5, 11);
        // Tuple created by txn 7 which is still active
        assert!(!is_visible(7, XMAX_NONE, &snap));
    }

    #[test]
    fn test_visible_created_by_own_txn() {
        let snap = make_snapshot(10, &[], 5, 11);
        // Created by our own transaction
        assert!(is_visible(10, XMAX_NONE, &snap));
    }

    #[test]
    fn test_invisible_created_after_snapshot() {
        let snap = make_snapshot(10, &[], 5, 11);
        // Created by txn 15, which started after our snapshot
        assert!(!is_visible(15, XMAX_NONE, &snap));
    }

    #[test]
    fn test_invisible_deleted_by_committed_txn() {
        let snap = make_snapshot(10, &[], 1, 11);
        // Created by txn 2 (committed), deleted by txn 8 (committed, not in active set)
        assert!(!is_visible(2, 8, &snap));
    }

    #[test]
    fn test_visible_deleted_by_active_txn() {
        let snap = make_snapshot(10, &[8], 1, 11);
        // Created by txn 2 (committed), deleted by txn 8 (still active)
        assert!(is_visible(2, 8, &snap));
    }

    #[test]
    fn test_invisible_deleted_by_own_txn() {
        let snap = make_snapshot(10, &[], 1, 11);
        // Created by txn 2 (committed), deleted by our own txn 10
        assert!(!is_visible(2, 10, &snap));
    }

    #[test]
    fn test_visible_deleted_by_future_txn() {
        let snap = make_snapshot(10, &[], 1, 11);
        // Created by txn 2 (committed), deleted by txn 15 (started after snapshot)
        assert!(is_visible(2, 15, &snap));
    }

    #[test]
    fn test_encode_decode_header() {
        let xmin = 42u64;
        let xmax = 100u64;
        let header = encode_version_header(xmin, xmax);
        let (decoded_xmin, decoded_xmax) = decode_version_header(&header);
        assert_eq!(decoded_xmin, xmin);
        assert_eq!(decoded_xmax, xmax);
    }

    #[test]
    fn test_encode_decode_header_zero() {
        let header = encode_version_header(0, 0);
        let (xmin, xmax) = decode_version_header(&header);
        assert_eq!(xmin, 0);
        assert_eq!(xmax, 0);
    }

    #[test]
    fn test_committed_in_window_visible() {
        // Txn 7 committed during the window (not in active set)
        let snap = make_snapshot(10, &[9], 5, 11);
        assert!(is_visible(7, XMAX_NONE, &snap));
    }

    #[test]
    fn test_all_xmin_xmax_combinations() {
        let snap = make_snapshot(10, &[6, 8], 5, 11);

        // xmin=3 (before window), xmax=0 -> visible
        assert!(is_visible(3, XMAX_NONE, &snap));

        // xmin=3, xmax=6 (active) -> visible (delete not committed)
        assert!(is_visible(3, 6, &snap));

        // xmin=3, xmax=7 (committed in window) -> invisible
        assert!(!is_visible(3, 7, &snap));

        // xmin=6 (active), xmax=0 -> invisible (creator not committed)
        assert!(!is_visible(6, XMAX_NONE, &snap));

        // xmin=7 (committed in window), xmax=0 -> visible
        assert!(is_visible(7, XMAX_NONE, &snap));

        // xmin=10 (self), xmax=0 -> visible
        assert!(is_visible(10, XMAX_NONE, &snap));

        // xmin=10 (self), xmax=10 (self deleted) -> invisible
        assert!(!is_visible(10, 10, &snap));

        // xmin=12 (future) -> invisible
        assert!(!is_visible(12, XMAX_NONE, &snap));
    }
}
